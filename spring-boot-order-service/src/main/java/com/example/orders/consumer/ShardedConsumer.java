package com.example.orders.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "app.consumer.enabled", havingValue = "true", matchIfMissing = true)
public class ShardedConsumer implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(ShardedConsumer.class);

    private final List<StatefulRedisConnection<String, String>> shards;
    private final JdbcTemplate jdbc;
    private final ObjectMapper objectMapper;
    private final int numShards;
    private final int batchSize;
    private final String streamBase;
    private final String groupName;
    private final String consumerName;
    private final int ackBatchSize;
    private final long ackFlushIntervalMs;

    // Per-shard ack buffers. Each shard runs in its own virtual thread and
    // only touches ackBuffers.get(shardIdx) — no cross-thread contention.
    private List<List<String>> ackBuffers;
    private long[] ackLastFlushMs;

    private static final String ACK_KEY_BASE = "orders:acks";
    private static final long   ACK_KEY_TTL  = 86_400L; // 24 h in seconds

    public ShardedConsumer(
            List<StatefulRedisConnection<String, String>> shards,
            JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            @Value("${app.num-shards}") int numShards,
            @Value("${app.batch-size:500}") int batchSize,
            @Value("${app.stream-base}") String streamBase,
            @Value("${app.group-name}") String groupName,
            @Value("${app.consumer-name}") String consumerName,
            @Value("${app.ack-batch-size:500}") int ackBatchSize,
            @Value("${app.ack-flush-interval-ms:5000}") long ackFlushIntervalMs) {
        this.shards = shards;
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.numShards = numShards;
        this.batchSize = batchSize;
        this.streamBase = streamBase;
        this.groupName = groupName;
        this.consumerName = consumerName;
        this.ackBatchSize = ackBatchSize;
        this.ackFlushIntervalMs = ackFlushIntervalMs;
    }

    @Override
    public void run(ApplicationArguments args) {
        createTable();
        ackBuffers = new ArrayList<>(numShards);
        for (int i = 0; i < numShards; i++) ackBuffers.add(new ArrayList<>());
        ackLastFlushMs = new long[numShards];
        for (int i = 0; i < numShards; i++) {
            final int shardIdx = i;
            Thread.ofVirtual()
                    .name("consumer-shard-" + shardIdx)
                    .start(() -> consumeShard(shardIdx));
        }
        log.info("Sharded consumer started on {} shards, batch-size={}", numShards, batchSize);
    }

    private void consumeShard(int shardIdx) {
        String streamName = streamBase + ":" + shardIdx;
        RedisCommands<String, String> commands = shards.get(shardIdx).sync();

        try {
            commands.xgroupCreate(
                    XReadArgs.StreamOffset.from(streamName, "$"),
                    groupName,
                    XGroupCreateArgs.Builder.mkstream()
            );
        } catch (Exception ignored) {
            // BUSYGROUP: group already exists — safe to continue
        }

        log.info("Shard {} consumer ready on stream {}", shardIdx, streamName);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                var messages = commands.xreadgroup(
                        Consumer.from(groupName, consumerName),
                        XReadArgs.Builder.count(batchSize).block(2000),
                        XReadArgs.StreamOffset.lastConsumed(streamName)
                );
                if (messages != null && !messages.isEmpty()) {
                    flushBatch(commands, streamName, shardIdx, messages);
                }
            } catch (Exception e) {
                log.error("Shard {} error: {}", shardIdx, e.getMessage());
                try { Thread.sleep(1_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Insert all messages from one xreadgroup poll in a single batchUpdate,
     * then XACK every message ID in one Redis command.
     *
     * Why this is faster than per-message inserts:
     *   - JdbcTemplate.batchUpdate sends all rows in one prepared-statement
     *     batch over a single DB connection — one round trip instead of N.
     *   - synchronous_commit=off (set via connection-init-sql in application.yml)
     *     tells Postgres to ack commits before WAL is flushed to disk,
     *     eliminating the fsync wait per transaction.
     *   - commands.xack(stream, group, id1, id2, ...) sends a single XACK
     *     with all IDs in varargs — one Redis round trip for the whole batch.
     *
     * At-least-once guarantee: XACK is called only after batchUpdate succeeds.
     * If the insert throws, messages stay in the PEL and are re-delivered.
     */
    private void flushBatch(RedisCommands<String, String> commands, String streamName,
                            int shardIdx, List<StreamMessage<String, String>> messages) {
        List<Object[]> batchArgs = new ArrayList<>(messages.size());
        List<String> msgIds = new ArrayList<>(messages.size());

        for (var msg : messages) {
            try {
                JsonNode data = objectMapper.readTree(msg.getBody().get("data"));
                batchArgs.add(new Object[]{
                        data.get("order_id").asText(),
                        data.get("customer_id").asText(),
                        data.get("amount").asDouble(),
                        objectMapper.writeValueAsString(data.get("items")),
                        msg.getId(),
                        shardIdx
                });
                msgIds.add(msg.getId());
            } catch (Exception e) {
                log.error("Skipping malformed message {}: {}", msg.getId(), e.getMessage());
            }
        }

        if (batchArgs.isEmpty()) return;

        jdbc.batchUpdate(
                "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) " +
                "VALUES (?, ?, ?, ?::jsonb, ?, ?)",
                batchArgs
        );

        // Varargs XACK — one command for the whole batch
        commands.xack(streamName, groupName, msgIds.toArray(new String[0]));
        log.debug("Shard {} flushed {} rows", shardIdx, batchArgs.size());

        // ── Ack buffer ───────────────────────────────────────────────────────
        // Accumulate order_ids and flush to Redis Hash when threshold is met.
        // Each shard's virtual thread is the sole writer of ackBuffers[shardIdx].
        List<String> ackBuf = ackBuffers.get(shardIdx);
        for (Object[] row : batchArgs) ackBuf.add((String) row[0]); // row[0] = order_id

        long now = System.currentTimeMillis();
        if (ackBuf.size() >= ackBatchSize || now - ackLastFlushMs[shardIdx] >= ackFlushIntervalMs) {
            String ackKey = ACK_KEY_BASE + ":" + shardIdx;
            Map<String, String> mapping = new HashMap<>(ackBuf.size());
            String ts = String.valueOf(now);
            for (String oid : ackBuf) mapping.put(oid, ts);
            commands.hset(ackKey, mapping);
            commands.expire(ackKey, ACK_KEY_TTL);
            log.debug("Shard {} ack flush count={}", shardIdx, ackBuf.size());
            ackBuf.clear();
            ackLastFlushMs[shardIdx] = now;
        }
    }

    private void createTable() {
        jdbc.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id          BIGSERIAL PRIMARY KEY,
                    order_id    TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    amount      NUMERIC(12, 2) NOT NULL,
                    items       JSONB NOT NULL,
                    msg_id      TEXT,
                    shard       INT,
                    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """);
    }
}
