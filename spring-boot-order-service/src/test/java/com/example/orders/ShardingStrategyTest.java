package com.example.orders;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ShardingStrategyTest {

    private static final int NUM_SHARDS = 4;

    private static int shardFor(String orderId, int numShards) {
        return Math.abs(orderId.hashCode()) % numShards;
    }

    @Test
    void shardIndexIsAlwaysWithinValidRange() {
        for (int i = 0; i < 1_000; i++) {
            int shard = shardFor("ORD-" + i, NUM_SHARDS);
            assertThat(shard).isBetween(0, NUM_SHARDS - 1);
        }
    }

    @Test
    void sameOrderIdAlwaysRoutesToSameShard() {
        String orderId = "ORD-FIXED-HASH-123";
        int expected = shardFor(orderId, NUM_SHARDS);
        for (int i = 0; i < 100; i++) {
            assertThat(shardFor(orderId, NUM_SHARDS)).isEqualTo(expected);
        }
    }

    @Test
    void differentOrderIdsCanLandOnDifferentShards() {
        // Verify at least 2 distinct shards appear in a set of orders
        long distinctShards = java.util.stream.IntStream.range(0, 100)
                .map(i -> shardFor("ORD-VARIED-" + i, NUM_SHARDS))
                .distinct()
                .count();
        assertThat(distinctShards).isGreaterThan(1);
    }

    @Test
    void distributionIsRoughlyEvenAcrossShards() {
        int total = 10_000;
        Map<Integer, Integer> counts = new HashMap<>();
        for (int i = 0; i < total; i++) {
            counts.merge(shardFor("ORD-" + i, NUM_SHARDS), 1, Integer::sum);
        }

        assertThat(counts).hasSize(NUM_SHARDS);

        // Each shard should receive 15%–35% of traffic (expected: 25%)
        int expected = total / NUM_SHARDS;
        counts.values().forEach(c ->
                assertThat(c).isBetween((int) (expected * 0.6), (int) (expected * 1.4)));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 4, 8, 16})
    void shardIsWithinRangeForAnyShardCount(int numShards) {
        for (int i = 0; i < 200; i++) {
            int shard = shardFor("ORD-SCALE-" + i, numShards);
            assertThat(shard).isBetween(0, numShards - 1);
        }
    }
}
