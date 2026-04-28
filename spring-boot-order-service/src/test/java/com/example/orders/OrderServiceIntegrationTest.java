package com.example.orders;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.codec.StringCodec;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@Testcontainers
class OrderServiceIntegrationTest {

    // Must be the very first static initializer so DOCKER_HOST is set before
    // the @Testcontainers extension starts the @Container fields below.
    static {
        configurePodman();
    }

    @Container
    static GenericContainer<?> redis = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379)
            .withCommand("redis-server", "--requirepass", "testpass");

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("orders_db")
            .withUsername("postgres")
            .withPassword("postgres123");

    @DynamicPropertySource
    static void overrideProperties(DynamicPropertyRegistry registry) {
        String redisUrl = "redis://:testpass@" + redis.getHost() + ":" + redis.getMappedPort(6379);

        // Pre-create consumer group at 0-0 so the app's consumer thread picks
        // up test messages regardless of virtual-thread scheduling order.
        try (var client = RedisClient.create(redisUrl);
             var conn = client.connect(StringCodec.UTF8)) {
            conn.sync().xgroupCreate(
                    XReadArgs.StreamOffset.from("orders:stream:0", "0-0"),
                    "order_processors",
                    XGroupCreateArgs.Builder.mkstream()
            );
        } catch (Exception ignored) {}

        registry.add("app.redis.urls", () -> redisUrl);
        registry.add("app.num-shards", () -> "1");
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
    }

    @Autowired
    WebTestClient webTestClient;

    @Autowired
    JdbcTemplate jdbc;

    @Test
    void postOrder_isQueuedOnRedisAndConsumerInsertsRow() {
        String orderId = "ORD-IT-001";

        webTestClient.post().uri("/orders")
                .bodyValue(Map.of(
                        "order_id", orderId,
                        "customer_id", "CUST-42",
                        "amount", 299.99,
                        "items", List.of("Laptop", "Mouse")))
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.status").isEqualTo("queued")
                .jsonPath("$.shard").isEqualTo("0");

        await().atMost(15, SECONDS).until(() ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM orders WHERE order_id = ?",
                        Long.class, orderId) > 0);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT order_id, customer_id, amount, shard FROM orders WHERE order_id = ?",
                orderId);

        assertThat(row.get("order_id")).isEqualTo(orderId);
        assertThat(row.get("customer_id")).isEqualTo("CUST-42");
        assertThat((BigDecimal) row.get("amount")).isEqualByComparingTo("299.99");
        assertThat(row.get("shard")).isEqualTo(0);
    }

    @Test
    void postMultipleOrders_allRowsAppearInDatabase() {
        List<String> orderIds = List.of("ORD-IT-BATCH-1", "ORD-IT-BATCH-2", "ORD-IT-BATCH-3");

        for (String orderId : orderIds) {
            webTestClient.post().uri("/orders")
                    .bodyValue(Map.of(
                            "order_id", orderId,
                            "customer_id", "CUST-BATCH",
                            "amount", 99.99,
                            "items", List.of("Headphones")))
                    .header("Content-Type", "application/json")
                    .exchange()
                    .expectStatus().isOk();
        }

        await().atMost(20, SECONDS).until(() ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM orders WHERE order_id LIKE 'ORD-IT-BATCH-%'",
                        Long.class) == orderIds.size());
    }

    @Test
    void postOrder_messageIsAckedAfterInsert() {
        String orderId = "ORD-IT-ACK";

        webTestClient.post().uri("/orders")
                .bodyValue(Map.of(
                        "order_id", orderId,
                        "customer_id", "CUST-ACK",
                        "amount", 49.99,
                        "items", List.of("Keyboard")))
                .header("Content-Type", "application/json")
                .exchange()
                .expectStatus().isOk();

        await().atMost(15, SECONDS).until(() ->
                jdbc.queryForObject(
                        "SELECT COUNT(*) FROM orders WHERE order_id = ?",
                        Long.class, orderId) > 0);

        // No pending (unacknowledged) messages should remain
        String redisUrl = "redis://:testpass@" + redis.getHost() + ":" + redis.getMappedPort(6379);
        try (var client = RedisClient.create(redisUrl);
             var conn = client.connect(StringCodec.UTF8)) {
            var pending = conn.sync().xpending(
                    "orders:stream:0",
                    "order_processors",
                    Range.unbounded(),
                    Limit.from(10)
            );
            assertThat(pending).isEmpty();
        }
    }

    // ── Podman socket detection ───────────────────────────────────────────────

    static void configurePodman() {
        // Primary config is ~/.testcontainers.properties (docker.host key).
        // This fallback covers CI or machines where that file is absent.
        if (System.getenv("DOCKER_HOST") != null) return;
        if (new File("/var/run/docker.sock").canRead()) return;

        try {
            Process p = new ProcessBuilder("id", "-u").start();
            String uid = new String(p.getInputStream().readAllBytes()).trim();
            String podmanSock = "/run/user/" + uid + "/podman/podman.sock";
            if (new File(podmanSock).exists()) {
                System.setProperty("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", podmanSock);
                System.setProperty("docker.host", "unix://" + podmanSock);
            }
        } catch (Exception ignored) {}
    }
}
