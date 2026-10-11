package com.github.sabomichal.pgadvisory;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import io.r2dbc.spi.Result;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Function;

/** One PostgreSQL container shared by all tests, with the lock table created by the script shipped in the jar. */
public final class TestPostgres {

    private static final String LOCK_TABLE_SCRIPT = "/db/pg-advisory/V1__advisory_lock.sql";
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    private static final PostgreSQLContainer CONTAINER = start();

    static {
        migrate();
    }

    private TestPostgres() {
    }

    private static PostgreSQLContainer start() {
        var container = new PostgreSQLContainer("postgres:16-alpine");
        container.start();
        return container;
    }

    /**
     * Runs the script shipped in the jar, like the migrations of an application would. Step by step on the calling
     * thread: it runs during the class initialization, and a lambda of this class called on a driver thread would
     * wait for that initialization to finish.
     */
    private static void migrate() {
        String script;
        try (var in = TestPostgres.class.getResourceAsStream(LOCK_TABLE_SCRIPT)) {
            script = new String(Objects.requireNonNull(in, LOCK_TABLE_SCRIPT).readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        var connection = await(Mono.from(connectionFactory().create()));
        try {
            Flux.from(connection.createStatement(script).execute()).concatMap(Result::getRowsUpdated).blockLast(TIMEOUT);
        } finally {
            await(Mono.from(connection.close()));
        }
    }

    /** Unpooled, suitable for the lock session. */
    public static ConnectionFactory connectionFactory() {
        return ConnectionFactories.get(options("r2dbc:postgresql"));
    }

    /** @param scheme e.g. {@code r2dbc:postgresql} or {@code r2dbc:pool:postgresql} */
    public static ConnectionFactoryOptions options(String scheme) {
        return ConnectionFactoryOptions.parse(url(scheme))
                .mutate()
                .option(ConnectionFactoryOptions.USER, username())
                .option(ConnectionFactoryOptions.PASSWORD, password())
                .build();
    }

    /** @param scheme e.g. {@code r2dbc:postgresql} or {@code r2dbc:pool:postgresql} */
    public static String url(String scheme) {
        return "%s://%s:%d/%s".formatted(scheme, CONTAINER.getHost(), CONTAINER.getMappedPort(5432), CONTAINER.getDatabaseName());
    }

    public static String username() {
        return CONTAINER.getUsername();
    }

    public static String password() {
        return CONTAINER.getPassword();
    }

    /** {@code spring.r2dbc.*} of the container, for {@code ApplicationContextRunner.withPropertyValues}. */
    public static String[] r2dbcProperties() {
        return new String[]{
                "spring.r2dbc.url=" + url("r2dbc:postgresql"),
                "spring.r2dbc.username=" + username(),
                "spring.r2dbc.password=" + password()};
    }

    public static ConnectionPool pool() {
        return new ConnectionPool(ConnectionPoolConfiguration.builder(connectionFactory()).maxSize(10).build());
    }

    public static <T> T await(Mono<T> mono) {
        return mono.block(TIMEOUT);
    }

    public static <T> Mono<T> withConnection(Function<Connection, Mono<T>> work) {
        return Mono.usingWhen(connectionFactory().create(), work, Connection::close);
    }

    public static Mono<Long> execute(Connection connection, String sql, Object... arguments) {
        var statement = connection.createStatement(sql);
        for (var i = 0; i < arguments.length; i++) {
            statement.bind(i, arguments[i]);
        }
        return Flux.from(statement.execute()).concatMap(result -> result.map(_ -> 1L)).count();
    }

    /** Kills the connections named {@code applicationName} and waits until they are gone; PostgreSQL releases their advisory locks. */
    public static long terminateConnections(String applicationName) {
        return await(withConnection(connection -> execute(connection,
                "SELECT pg_terminate_backend(pid, 10000) FROM pg_stat_activity WHERE application_name = $1",
                applicationName)));
    }
}
