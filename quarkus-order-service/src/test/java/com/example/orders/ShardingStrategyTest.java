package com.example.orders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ShardingStrategyTest {

    private static final int NUM_SHARDS = 4;

    private static int shardFor(String orderId, int numShards) {
        return Math.abs(orderId.hashCode()) % numShards;
    }

    @Test
    void shardIndexIsAlwaysWithinValidRange() {
        for (int i = 0; i < 1_000; i++) {
            int shard = shardFor("ORD-" + i, NUM_SHARDS);
            assertTrue(shard >= 0 && shard < NUM_SHARDS,
                    "Shard " + shard + " out of range for ORD-" + i);
        }
    }

    @Test
    void sameOrderIdAlwaysRoutesToSameShard() {
        String orderId = "ORD-FIXED-HASH-123";
        int expected = shardFor(orderId, NUM_SHARDS);
        for (int i = 0; i < 100; i++) {
            assertEquals(expected, shardFor(orderId, NUM_SHARDS));
        }
    }

    @Test
    void differentOrderIdsCanLandOnDifferentShards() {
        long distinctShards = java.util.stream.IntStream.range(0, 100)
                .map(i -> shardFor("ORD-VARIED-" + i, NUM_SHARDS))
                .distinct()
                .count();
        assertTrue(distinctShards > 1, "Expected multiple shards to be used");
    }

    @Test
    void distributionIsRoughlyEvenAcrossShards() {
        int total = 10_000;
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < total; i++) {
            counts.merge(shardFor("ORD-" + i, NUM_SHARDS), 1, Integer::sum);
        }

        assertEquals(NUM_SHARDS, counts.size());

        int expected = total / NUM_SHARDS;
        counts.values().forEach(c -> assertTrue(
                c >= (int) (expected * 0.6) && c <= (int) (expected * 1.4),
                "Shard count " + c + " is too far from expected " + expected));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8, 16})
    void shardIsWithinRangeForAnyShardCount(int numShards) {
        for (int i = 0; i < 200; i++) {
            int shard = shardFor("ORD-SCALE-" + i, numShards);
            assertTrue(shard >= 0 && shard < numShards);
        }
    }
}
