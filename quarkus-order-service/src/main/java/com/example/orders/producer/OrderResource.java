package com.example.orders.producer;

import com.example.orders.config.RedisConfig;
import com.example.orders.model.Order;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.XAddArgs;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.time.Instant;
import java.util.Map;

@Path("/orders")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class OrderResource {

    @Inject
    RedisConfig redisConfig;

    @Inject
    ObjectMapper objectMapper;

    @ConfigProperty(name = "app.producer.enabled", defaultValue = "true")
    boolean producerEnabled;

    @ConfigProperty(name = "app.num-shards")
    int numShards;

    @ConfigProperty(name = "app.stream-base")
    String streamBase;

    @POST
    @RunOnVirtualThread
    public Response createOrder(Order order) throws Exception {
        if (!producerEnabled) {
            return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                    .entity(Map.of("error", "producer disabled — this node runs consumer only"))
                    .build();
        }
        int shardIdx = Math.abs(order.orderId().hashCode()) % numShards;
        String streamName = streamBase + ":" + shardIdx;

        String data = objectMapper.writeValueAsString(order);
        Map<String, String> fields = Map.of(
                "type", "order_created",
                "data", data,
                "timestamp", String.valueOf(Instant.now().toEpochMilli())
        );

        // Blocking xadd on a virtual thread — no OS thread consumed during I/O wait
        String msgId = redisConfig.get(shardIdx).sync().xadd(
                streamName,
                XAddArgs.Builder.maxlen(1_000_000).approximateTrimming(),
                fields
        );

        return Response.ok(Map.of(
                "status", "queued",
                "message_id", msgId,
                "shard", shardIdx
        )).build();
    }
}
