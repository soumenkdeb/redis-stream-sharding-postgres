package com.example.orders.consumer;

import com.example.orders.config.RedisConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.Consumer;
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
import java.util.Map;

@ApplicationScoped
public class ShardedConsumer {

    private static final Logger log = Logger.getLogger(ShardedConsumer.class);

    @Inject
    RedisConfig redisConfig;

    @Inject
    AgroalDataSource dataSource;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "app.consumer.enabled", defaultValue = "true")
    boolean consumerEnabled;

    @ConfigProperty(name = "app.num-shards")
    int numShards;

    @ConfigProperty(name = "app.stream-base")
    String streamBase;

    @ConfigProperty(name = "app.group-name")
    String groupName;

    @ConfigProperty(name = "app.consumer-name")
    String consumerName;

    void onStart(@Observes StartupEvent ev) {
        if (!consumerEnabled) {
            log.info("Consumer disabled via app.consumer.enabled=false — skipping startup");
            return;
        }
        createTable();
        for (int i = 0; i < numShards; i++) {
            final int shardIdx = i;
            Thread.ofVirtual()
                    .name("consumer-shard-" + shardIdx)
                    .start(() -> consumeShard(shardIdx));
        }
        log.infof("Sharded consumer started on %d shards", numShards);
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
            // group already exists
        }

        log.infof("Shard %d consumer ready on %s", shardIdx, streamName);

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
                log.errorf("Shard %d error: %s", shardIdx, e.getMessage());
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
            try (Connection conn = dataSource.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "INSERT INTO orders (order_id, customer_id, amount, items, msg_id, shard) " +
                         "VALUES (?, ?, ?, ?::jsonb, ?, ?)")) {
                ps.setString(1, data.get("order_id").asText());
                ps.setString(2, data.get("customer_id").asText());
                ps.setDouble(3, data.get("amount").asDouble());
                ps.setString(4, objectMapper.writeValueAsString(data.get("items")));
                ps.setString(5, msgId);
                ps.setInt(6, shardIdx);
                ps.executeUpdate();
            }
            commands.xack(streamName, groupName, msgId);
        } catch (Exception e) {
            log.errorf("Failed to process message %s: %s", msgId, e.getMessage());
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
