package com.example.orders.infrastructure;

import io.lettuce.core.RedisClient;
import io.lettuce.core.XGroupCreateArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.codec.StringCodec;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.File;
import java.util.Map;

/**
 * Starts Redis + PostgreSQL Testcontainers before Quarkus boots, injects
 * their coordinates as system properties that override application.properties.
 * Also pre-creates the consumer group so test messages are always picked up
 * regardless of virtual-thread scheduling order at startup.
 */
public class RedisPostgresResource implements QuarkusTestResourceLifecycleManager {

    static {
        // Must run before any Testcontainers object is used.
        // Rootless Podman exposes its socket at /run/user/<uid>/podman/podman.sock
        // rather than /var/run/docker.sock — point Testcontainers there.
        configurePodman();
    }

    public static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7-alpine")
            .withExposedPorts(6379)
            .withCommand("redis-server", "--requirepass", "testpass");

    public static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("orders_db")
            .withUsername("postgres")
            .withPassword("postgres123");

    @Override
    public Map<String, String> start() {
        REDIS.start();
        POSTGRES.start();

        String redisUrl = "redis://:testpass@" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379);

        // Pre-create group at 0-0 so the consumer picks up messages produced during tests
        try (var client = RedisClient.create(redisUrl);
             var conn = client.connect(StringCodec.UTF8)) {
            conn.sync().xgroupCreate(
                    XReadArgs.StreamOffset.from("orders:stream:0", "0-0"),
                    "order_processors",
                    XGroupCreateArgs.Builder.mkstream()
            );
        } catch (Exception ignored) {}

        return Map.of(
                "app.redis.urls", redisUrl,
                "app.num-shards", "1",
                "quarkus.datasource.jdbc.url", POSTGRES.getJdbcUrl(),
                "quarkus.datasource.username", POSTGRES.getUsername(),
                "quarkus.datasource.password", POSTGRES.getPassword()
        );
    }

    @Override
    public void stop() {
        REDIS.stop();
        POSTGRES.stop();
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
                // TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE is read via System.getProperty
                // by UnixSocketClientProviderStrategy in Testcontainers 1.17+
                System.setProperty("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", podmanSock);
                // docker.host is read from testcontainers.properties; set it as a
                // system property too as an extra fallback for some TC versions
                System.setProperty("docker.host", "unix://" + podmanSock);
            }
        } catch (Exception ignored) {}
    }
}
