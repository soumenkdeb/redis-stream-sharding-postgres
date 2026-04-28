package com.example.orders.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.Arrays;
import java.util.List;

/**
 * Holds one Lettuce connection per Redis shard.
 * Injected directly so CDI doesn't have to handle raw generic List types.
 */
@ApplicationScoped
public class RedisConfig {

    @ConfigProperty(name = "app.redis.urls")
    String redisUrls;

    private List<StatefulRedisConnection<String, String>> connections;

    @PostConstruct
    void init() {
        connections = Arrays.stream(redisUrls.split(","))
                .map(url -> RedisClient.create(url.trim())
                        .<String, String>connect(StringCodec.UTF8))
                .toList();
    }

    public StatefulRedisConnection<String, String> get(int shardIdx) {
        return connections.get(shardIdx);
    }
}
