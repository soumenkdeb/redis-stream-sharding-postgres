package com.example.orders.config;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.StringCodec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;

@Configuration
public class RedisConfig {

    @Bean
    public List<StatefulRedisConnection<String, String>> redisConnections(
            @Value("${app.redis.urls}") String redisUrls) {
        return Arrays.stream(redisUrls.split(","))
                .map(url -> RedisClient.create(url.trim())
                        .<String, String>connect(StringCodec.UTF8))
                .toList();
    }
}
