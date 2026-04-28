package com.example.orders.producer;

import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@WebFluxTest(OrderController.class)
class OrderControllerTest {

    static final String FAKE_MSG_ID = "1714000000000-0";

    @TestConfiguration
    static class MockRedisConfig {

        @Bean
        @SuppressWarnings("unchecked")
        List<StatefulRedisConnection<String, String>> redisConnections() {
            RedisReactiveCommands<String, String> reactive =
                    (RedisReactiveCommands<String, String>) mock(RedisReactiveCommands.class);
            when(reactive.xadd(anyString(), any(XAddArgs.class), anyMap()))
                    .thenReturn(Mono.just(FAKE_MSG_ID));

            StatefulRedisConnection<String, String> conn =
                    (StatefulRedisConnection<String, String>) mock(StatefulRedisConnection.class);
            when(conn.reactive()).thenReturn(reactive);

            // 4 connections for 4 shards
            return List.of(conn, conn, conn, conn);
        }
    }

    @Autowired
    WebTestClient webTestClient;

    @Test
    void postOrder_returns200WithQueuedStatus() {
        webTestClient.post().uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "order_id", "ORD-001",
                        "customer_id", "CUST-42",
                        "amount", 199.99,
                        "items", List.of("Laptop", "Mouse")))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("queued")
                .jsonPath("$.message_id").isEqualTo(FAKE_MSG_ID)
                .jsonPath("$.shard").exists();
    }

    @Test
    void postOrder_shardIsDeterministicForSameOrderId() {
        int expectedShard = Math.abs("ORD-STABLE".hashCode()) % 4;

        webTestClient.post().uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "order_id", "ORD-STABLE",
                        "customer_id", "CUST-1",
                        "amount", 10.0,
                        "items", List.of("Keyboard")))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.shard").isEqualTo(String.valueOf(expectedShard));
    }

    @Test
    void postOrder_missingBody_returns4xx() {
        webTestClient.post().uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .exchange()
                .expectStatus().is4xxClientError();
    }

    @Test
    void postOrder_allResponseFieldsPresent() {
        webTestClient.post().uri("/orders")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(Map.of(
                        "order_id", "ORD-FIELDS",
                        "customer_id", "CUST-99",
                        "amount", 49.99,
                        "items", List.of("Mouse")))
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").exists()
                .jsonPath("$.message_id").exists()
                .jsonPath("$.shard").exists();
    }
}
