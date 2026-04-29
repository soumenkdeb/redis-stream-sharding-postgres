package com.example.orders.producer;

import com.example.orders.config.RedisConfig;
import io.lettuce.core.KeyValue;
import io.smallrye.common.annotation.RunOnVirtualThread;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;

/**
 * GET /ack/status?ids=id1,id2,...
 *   Returns {acked, pending, pct_complete, orders:{id→"acked"|"pending"}}
 *
 * GET /ack/summary
 *   Returns total acked count per shard (HLEN on each orders:acks:{shard}).
 *
 * The consumer writes order_ids to orders:acks:{shard} after each successful
 * DB batch commit.  This endpoint lets the load test or monitoring systems
 * track end-to-end processing completion without querying Postgres directly.
 */
@Path("/ack")
@Produces(MediaType.APPLICATION_JSON)
public class AckResource {

    private static final String ACK_KEY_BASE = "orders:acks";
    private static final int    MAX_IDS      = 10_000;

    @Inject
    RedisConfig redisConfig;

    @ConfigProperty(name = "app.num-shards")
    int numShards;

    /**
     * Routes each order_id to its shard using the same hash as the producer,
     * then issues one HMGET per shard.
     */
    @GET
    @Path("/status")
    @RunOnVirtualThread
    public Response status(@QueryParam("ids") String ids) {
        if (ids == null || ids.isBlank())
            return Response.status(400).entity(Map.of("error", "ids param required")).build();

        List<String> orderIds = Arrays.stream(ids.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        if (orderIds.isEmpty())
            return Response.status(400).entity(Map.of("error", "ids param required")).build();
        if (orderIds.size() > MAX_IDS)
            return Response.status(400).entity(Map.of("error", "max " + MAX_IDS + " ids per request")).build();

        // Group by shard — one HMGET per shard instead of one per order_id
        Map<Integer, List<String>> byShardMap = new HashMap<>();
        for (String oid : orderIds)
            byShardMap.computeIfAbsent(getShard(oid), k -> new ArrayList<>()).add(oid);

        Map<String, String> result = new HashMap<>(orderIds.size());
        for (var entry : byShardMap.entrySet()) {
            int shardIdx = entry.getKey();
            List<String> shardIds = entry.getValue();
            String ackKey = ACK_KEY_BASE + ":" + shardIdx;
            List<KeyValue<String, String>> values =
                    redisConfig.get(shardIdx).sync().hmget(ackKey, shardIds.toArray(new String[0]));
            for (KeyValue<String, String> kv : values)
                result.put(kv.getKey(), kv.hasValue() ? "acked" : "pending");
        }

        long acked = result.values().stream().filter("acked"::equals).count();
        long total = result.size();
        double pct = total > 0 ? Math.round(acked * 10_000.0 / total) / 100.0 : 0.0;

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("total", total);
        out.put("acked", acked);
        out.put("pending", total - acked);
        out.put("pct_complete", pct);
        out.put("orders", result);
        return Response.ok(out).build();
    }

    /** HLEN on each shard's ack hash — fast count without listing IDs. */
    @GET
    @Path("/summary")
    @RunOnVirtualThread
    public Map<String, Object> summary() {
        List<Map<String, Object>> byShardList = new ArrayList<>(numShards);
        long totalAcked = 0;
        for (int i = 0; i < numShards; i++) {
            long count = redisConfig.get(i).sync().hlen(ACK_KEY_BASE + ":" + i);
            byShardList.add(Map.of("shard", i, "acked", count));
            totalAcked += count;
        }
        return Map.of("total_acked", totalAcked, "by_shard", byShardList);
    }

    private int getShard(String orderId) {
        // Must match OrderResource routing — Java String.hashCode() is
        // spec-defined and consistent across all JVM processes.
        return Math.abs(orderId.hashCode()) % numShards;
    }
}
