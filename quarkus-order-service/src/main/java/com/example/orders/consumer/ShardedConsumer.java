package com.example.orders.consumer;

import com.example.orders.config.RedisConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Consumer;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.agroal.api.AgroalDataSource;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ApplicationScoped
public class ShardedConsumer {

    private static final Logger log = Logger.getLogger(ShardedConsumer.class);

    @Inject RedisConfig redisConfig;
    @Inject AgroalDataSource dataSource;
    @Inject ObjectMapper objectMapper;

    @ConfigProperty(name = "app.consumer.enabled", defaultValue = "true")
    boolean consumerEnabled;

    @ConfigProperty(name = "app.num-shards")
    int numShards;

    @ConfigProperty(name = "app.batch-size", defaultValue = "500")
    int batchSize;

    @ConfigProperty(name = "app.stream-base")
    String streamBase;

    @ConfigProperty(name = "app.group-name")
    String groupName;

    @ConfigProperty(name = "app.consumer-name")
    String consumerName;

    @ConfigProperty(name = "app.ack-batch-size", defaultValue = "500")
    int ackBatchSize;

    @ConfigProperty(name = "app.ack-flush-interval-ms", defaultValue = "5000")
    long ackFlushIntervalMs;

    // Per-shard ack buffers — each shard's virtual thread is sole writer of its slot
    private List<List<String>> ackBuffers;
    private long[] ackLastFlushMs;

    private static final String ACK_KEY_BASE = "orders:acks";
    private static final long   ACK_KEY_TTL  = 86_400L;

    void onStart(@Observes StartupEvent ev) {
        if (!consumerEnabled) {
            log.info("Consumer disabled via app.consumer.enabled=false — skipping startup");
            return;
        }
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
        log.infof("Sharded consumer started on %d shards, batch-size=%d", numShards, batchSize);
    }

    private void consumeShard(int shardIdx) {
        String streamName = streamBase + ":" + shardIdx;
        RedisCommands<String, String> commands = redisConfig.get(shardIdx).sync();

        try {
            commands.xgroupCreate(
                    XReadArgs.StreamOffset.from(streamName, "$"),
                    groupName,
                    XGroupCreateArgs.Builder.mkstream()
            );
        } catch (Exception ignored) {
            // BUSYGROUP: group already exists — safe to continue
        }

        log.infof("Shard %d consumer ready on %s", shardIdx, streamName);

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
                log.errorf("Shard %d error: %s", shardIdx, e.getMessage());
                try { Thread.sleep(1_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Insert all messages from one xreadgroup poll as a JDBC batch, then XACK
     * all IDs in one Redis command.
     *
     * Why this is faster than per-message inserts:
     *   - addBatch() / executeBatch() sends all rows in one prepared-statement
     *     batch — one DB round trip instead of N.
     *   - autoCommit=false wraps the whole batch in one transaction, so Postgres
     *     writes WAL once per batch rather than once per row.
     *   - synchronous_commit=off (set via new-connection-sql in application.properties)
     *     removes the fsync wait on every commit.
     *   - commands.xack with varargs sends a single XACK for all IDs.
     *
     * At-least-once guarantee: if executeBatch() throws, the connection is rolled
     * back and XACK is not called — messages stay in the PEL for re-delivery.
     */
    private void flushBatch(RedisCommands<String, String> commands, String streamName,
                            int shardIdx, List<StreamMessage<String, String>> messages) {
        List<String> msgIds    = new ArrayList<>(messages.size()); // Redis stream message IDs
        List<String> orderIds  = new ArrayList<>(messages.size()); // business order_ids for ack hash

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) " +
                    "VALUES (?, ?, ?, ?::jsonb, ?, ?)")) {

                for (var msg : messages) {
                    try {
                        JsonNode data = objectMapper.readTree(msg.getBody().get("data"));
                        String orderId = data.get("order_id").asText();
                        ps.setString(1, orderId);
                        ps.setString(2, data.get("customer_id").asText());
                        ps.setDouble(3, data.get("amount").asDouble());
                        ps.setString(4, objectMapper.writeValueAsString(data.get("items")));
                        ps.setString(5, msg.getId());
                        ps.setInt(6, shardIdx);
                        ps.addBatch();
                        msgIds.add(msg.getId());
                        orderIds.add(orderId);
                    } catch (Exception e) {
                        log.errorf("Skipping malformed message %s: %s", msg.getId(), e.getMessage());
                    }
                }

                ps.executeBatch();
            }

            conn.commit();

        } catch (Exception e) {
            log.errorf("Batch insert failed for shard %d: %s", shardIdx, e.getMessage());
            return; // do not XACK — messages stay in PEL for re-delivery
        }

        if (!msgIds.isEmpty()) {
            commands.xack(streamName, groupName, msgIds.toArray(new String[0]));
            log.infof("Shard %d consumed %d messages, inserted %d rows", shardIdx, messages.size(), msgIds.size());

            // ── Ack buffer ───────────────────────────────────────────────────
            // Each shard's virtual thread is sole writer of ackBuffers[shardIdx].
            List<String> ackBuf = ackBuffers.get(shardIdx);
            ackBuf.addAll(orderIds);

            long now = System.currentTimeMillis();
            if (ackBuf.size() >= ackBatchSize || now - ackLastFlushMs[shardIdx] >= ackFlushIntervalMs) {
                String ackKey = ACK_KEY_BASE + ":" + shardIdx;
                Map<String, String> mapping = new HashMap<>(ackBuf.size());
                String ts = String.valueOf(now);
                for (String oid : ackBuf) mapping.put(oid, ts);
                commands.hset(ackKey, mapping);
                commands.expire(ackKey, ACK_KEY_TTL);
                log.infof("Shard %d flushed %d ACKs to Redis", shardIdx, ackBuf.size());
                ackBuf.clear();
                ackLastFlushMs[shardIdx] = now;
            }
        }
    }

    private void createTable() {
        try (Connection conn = dataSource.getConnection();
             var stmt = conn.createStatement()) {
            stmt.execute("""
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
        } catch (Exception e) {
            throw new RuntimeException("Failed to create orders table", e);
        }
    }
}
