package com.example.orders.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Consumer;
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
    private final String streamBase;
    private final String groupName;
    private final String consumerName;

    public ShardedConsumer(
            List<StatefulRedisConnection<String, String>> shards,
            JdbcTemplate jdbc,
            ObjectMapper objectMapper,
            @Value("${app.num-shards}") int numShards,
            @Value("${app.stream-base}") String streamBase,
            @Value("${app.group-name}") String groupName,
            @Value("${app.consumer-name}") String consumerName) {
        this.shards = shards;
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
        this.numShards = numShards;
        this.streamBase = streamBase;
        this.groupName = groupName;
        this.consumerName = consumerName;
    }

    @Override
    public void run(ApplicationArguments args) {
        createTable();
        for (int i = 0; i < numShards; i++) {
            final int shardIdx = i;
            Thread.ofVirtual()
                    .name("consumer-shard-" + shardIdx)
                    .start(() -> consumeShard(shardIdx));
        }
        log.info("Sharded consumer started on {} shards", numShards);
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
            // group already exists
        }

        log.info("Shard {} consumer ready on stream {}", shardIdx, streamName);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                var messages = commands.xreadgroup(
                        Consumer.from(groupName, consumerName),
                        XReadArgs.Builder.count(50).block(2000),
                        XReadArgs.StreamOffset.lastConsumed(streamName)
                );
                if (messages != null) {
                    for (var msg : messages) {
                        processMessage(commands, streamName, shardIdx,
                                msg.getId(), msg.getBody());
                    }
                }
            } catch (Exception e) {
                log.error("Shard {} error: {}", shardIdx, e.getMessage());
                try { Thread.sleep(1_000); } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private void processMessage(RedisCommands<String, String> commands,
                                String streamName, int shardIdx,
                                String msgId, Map<String, String> fields) {
        try {
            JsonNode data = objectMapper.readTree(fields.get("data"));
            jdbc.update(
                    "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) " +
                    "VALUES (?, ?, ?, ?::jsonb, ?, ?)",
                    data.get("order_id").asText(),
                    data.get("customer_id").asText(),
                    data.get("amount").asDouble(),
                    objectMapper.writeValueAsString(data.get("items")),
                    msgId,
                    shardIdx
            );
            commands.xack(streamName, groupName, msgId);
        } catch (Exception e) {
            log.error("Failed to process message {}: {}", msgId, e.getMessage());
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
