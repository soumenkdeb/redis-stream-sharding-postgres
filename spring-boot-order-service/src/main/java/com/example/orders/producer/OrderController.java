package com.example.orders.producer;

import com.example.orders.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/orders")
@ConditionalOnProperty(name = "app.producer.enabled", havingValue = "true", matchIfMissing = true)
public class OrderController {

    private final List<StatefulRedisConnection<String, String>> shards;
    private final ObjectMapper objectMapper;
    private final int numShards;
    private final String streamBase;

    public OrderController(
            List<StatefulRedisConnection<String, String>> shards,
            ObjectMapper objectMapper,
            @Value("${app.num-shards}") int numShards,
            @Value("${app.stream-base}") String streamBase) {
        this.shards = shards;
        this.objectMapper = objectMapper;
        this.numShards = numShards;
        this.streamBase = streamBase;
    }

    @PostMapping
    public Mono<Map<String, String>> createOrder(@RequestBody Order order) {
        int shardIdx = Math.abs(order.orderId().hashCode()) % numShards;
        String streamName = streamBase + ":" + shardIdx;

        return Mono.fromCallable(() -> objectMapper.writeValueAsString(order))
                .subscribeOn(Schedulers.boundedElastic())
                .flatMap(data -> {
                    Map<String, String> fields = Map.of(
                            "type", "order_created",
                            "data", data,
                            "timestamp", String.valueOf(Instant.now().toEpochMilli())
                    );
                    // reactive() multiplexes over the single Netty connection per shard
                    return shards.get(shardIdx).reactive().xadd(
                            streamName,
                            XAddArgs.Builder.maxlen(1_000_000).approximateTrimming(),
                            fields
                    );
                })
                .map(msgId -> Map.of(
                        "status", "queued",
                        "message_id", msgId,
                        "shard", String.valueOf(shardIdx)
                ));
    }
}
