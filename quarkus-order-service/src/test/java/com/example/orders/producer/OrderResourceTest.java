package com.example.orders.producer;

import com.example.orders.infrastructure.RedisPostgresResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

@QuarkusTest
@QuarkusTestResource(RedisPostgresResource.class)
class OrderResourceTest {

    // DB access goes through the container directly to avoid CDI lifecycle issues
    // with @Inject DataSource during Quarkus's build-time augmentation step.

    @Test
    void postOrder_returns200WithQueuedStatus() {
        given()
            .contentType(ContentType.JSON)
            .body(Map.of(
                    "order_id", "ORD-Q-001",
                    "customer_id", "CUST-42",
                    "amount", 199.99,
                    "items", List.of("Laptop")))
        .when()
            .post("/orders")
        .then()
            .statusCode(200)
            .body("status", equalTo("queued"))
            .body("message_id", notNullValue())
            .body("shard", notNullValue());
    }

    @Test
    void postOrder_shardIsDeterministicForSameOrderId() {
        String orderId = "ORD-Q-STABLE";
        // num-shards=1 in tests, so all orders go to shard 0
        int expectedShard = Math.abs(orderId.hashCode()) % 1;

        given()
            .contentType(ContentType.JSON)
            .body(Map.of(
                    "order_id", orderId,
                    "customer_id", "CUST-1",
                    "amount", 10.0,
                    "items", List.of("Item")))
        .when()
            .post("/orders")
        .then()
            .statusCode(200)
            .body("shard", equalTo(expectedShard));
    }

    @Test
    void postOrder_isProcessedToDatabase() throws Exception {
        String orderId = "ORD-Q-DB-001";

        given()
            .contentType(ContentType.JSON)
            .body(Map.of(
                    "order_id", orderId,
                    "customer_id", "CUST-99",
                    "amount", 499.99,
                    "items", List.of("Tablet", "Case")))
        .when()
            .post("/orders")
        .then()
            .statusCode(200);

        await().atMost(15, SECONDS).until(() -> rowCount("order_id = ?", orderId) > 0);

        try (var conn = openDbConnection();
             var ps = conn.prepareStatement(
                     "SELECT order_id, customer_id, amount, shard FROM orders WHERE order_id = ?")) {
            ps.setString(1, orderId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next(), "Expected row for " + orderId);
                assertEquals(orderId,    rs.getString("order_id"));
                assertEquals("CUST-99",  rs.getString("customer_id"));
                assertEquals(499.99,     rs.getDouble("amount"), 0.01);
                assertEquals(0,          rs.getInt("shard"));
            }
        }
    }

    @Test
    void postMultipleOrders_allProcessedToDatabase() {
        List<String> orderIds = List.of("ORD-Q-BATCH-A", "ORD-Q-BATCH-B", "ORD-Q-BATCH-C");

        for (String orderId : orderIds) {
            given()
                .contentType(ContentType.JSON)
                .body(Map.of(
                        "order_id", orderId,
                        "customer_id", "CUST-BATCH",
                        "amount", 29.99,
                        "items", List.of("Smartwatch")))
            .when()
                .post("/orders")
            .then()
                .statusCode(200);
        }

        await().atMost(20, SECONDS)
               .until(() -> rowCount("order_id LIKE ?", "ORD-Q-BATCH-%") == orderIds.size());
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private java.sql.Connection openDbConnection() throws Exception {
        return DriverManager.getConnection(
                RedisPostgresResource.POSTGRES.getJdbcUrl(),
                RedisPostgresResource.POSTGRES.getUsername(),
                RedisPostgresResource.POSTGRES.getPassword());
    }

    private long rowCount(String where, String param) {
        try (var conn = openDbConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT COUNT(*) FROM orders WHERE " + where)) {
            ps.setString(1, param);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0;
            }
        } catch (Exception e) {
            return 0;
        }
    }
}
