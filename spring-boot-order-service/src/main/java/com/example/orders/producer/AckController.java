package com.example.orders.producer;

import io.lettuce.core.KeyValue;
import io.lettuce.core.api.StatefulRedisConnection;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;

import java.util.*;

/**
 * GET /ack/status?ids=id1,id2,...
 *   Returns {acked, pending, pct_complete, orders:{id→"acked"|"pending"}}
 *
 * GET /ack/summary
 *   Returns total acked count per shard (HLEN on each orders:acks:{shard}).
 *
 * The consumer writes order_ids to orders:acks:{shard} after each successful
 * DB batch commit.  This endpoint lets the load test (or any caller) check
 * processing completion without querying Postgres directly.
 *
 * Not conditional on app.producer.enabled — always active so consumer-only
 * nodes can also expose ack status for monitoring.
 */
@RestController
@RequestMapping("/ack")
public class AckController {

    private static final String ACK_KEY_BASE = "orders:acks";
    private static final int    MAX_IDS      = 10_000;

    private final List<StatefulRedisConnection<String, String>> shards;
    private final int numShards;

    public AckController(
            List<StatefulRedisConnection<String, String>> shards,
            @Value("${app.num-shards}") int numShards) {
        this.shards = shards;
        this.numShards = numShards;
    }

    /**
     * Routes each order_id to its shard using the same hash as the producer,
     * then issues one HMGET per shard.
     */
    @GetMapping("/status")
    public Map<String, Object> status(@RequestParam String ids) {
        List<String> orderIds = Arrays.stream(ids.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).toList();

        if (orderIds.isEmpty())
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ids param required");
        if (orderIds.size() > MAX_IDS)
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "max " + MAX_IDS + " ids per request");

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
                    shards.get(shardIdx).sync().hmget(ackKey, shardIds.toArray(new String[0]));
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
        return out;
    }

    /** HLEN on each shard's ack hash — fast count without listing IDs. */
    @GetMapping("/summary")
    public Map<String, Object> summary() {
        List<Map<String, Object>> byShardList = new ArrayList<>(numShards);
        long totalAcked = 0;
        for (int i = 0; i < numShards; i++) {
            long count = shards.get(i).sync().hlen(ACK_KEY_BASE + ":" + i);
            byShardList.add(Map.of("shard", i, "acked", count));
            totalAcked += count;
        }
        return Map.of("total_acked", totalAcked, "by_shard", byShardList);
    }

    private int getShard(String orderId) {
        // Must match OrderController routing — Java String.hashCode() is
        // spec-defined and consistent across all JVM processes.
        return Math.abs(orderId.hashCode()) % numShards;
    }
}
