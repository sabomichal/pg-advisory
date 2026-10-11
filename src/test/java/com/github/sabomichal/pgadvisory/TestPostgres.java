
package com.github.sabomichal.pgadvisory;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.core.io.ClassPathResource;
import org.springframework.r2dbc.connection.init.ResourceDatabasePopulator;
import org.testcontainers.postgresql.PostgreSQLContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.function.Function;

/**
 * Shared PostgreSQL test container with the advisory lock schema initialized.
 */
public final class TestPostgres {

    private static final Duration TIMEOUT = Duration.ofSeconds(30);
    private static final PostgreSQLContainer CONTAINER = new PostgreSQLContainer("postgres:18-alpine");
    private static final ConnectionFactory CONNECTION_FACTORY;
    private static final String SELECT_TERMINATED_SQL = """
            SELECT count(*)
            FROM (
                SELECT pg_terminate_backend(pid, 10000) AS terminated
                FROM pg_stat_activity
                WHERE application_name = $1
                  AND pid <> pg_backend_pid()
            ) t
            WHERE terminated
            """;

    static {
        CONTAINER.start();
        // the options need the mapped port of the started container
        CONNECTION_FACTORY = ConnectionFactories.get(options("r2dbc:postgresql"));
        await(new ResourceDatabasePopulator(
                new ClassPathResource("db/pg-advisory/V1__advisory_lock.sql"))
                .populate(CONNECTION_FACTORY));
    }

    private TestPostgres() {
    }

    /**
     * Unpooled connection factory for dedicated lock sessions.
     */
    public static ConnectionFactory connectionFactory() {
        return CONNECTION_FACTORY;
    }

    public static ConnectionFactoryOptions options(String scheme) {
        return ConnectionFactoryOptions.parse(url(scheme))
                .mutate()
                .option(ConnectionFactoryOptions.USER, username())
                .option(ConnectionFactoryOptions.PASSWORD, password())
                .build();
    }

    public static String url(String scheme) {
        return "%s://%s:%d/%s".formatted(
                scheme,
                CONTAINER.getHost(),
                CONTAINER.getMappedPort(5432),
                CONTAINER.getDatabaseName());
    }

    public static String username() {
        return CONTAINER.getUsername();
    }

    public static String password() {
        return CONTAINER.getPassword();
    }

    public static String[] r2dbcProperties() {
        return new String[]{
                "spring.r2dbc.url=" + url("r2dbc:postgresql"),
                "spring.r2dbc.username=" + username(),
                "spring.r2dbc.password=" + password()
        };
    }

    public static ConnectionPool pool() {
        return new ConnectionPool(ConnectionPoolConfiguration.builder(CONNECTION_FACTORY)
                .maxSize(10)
                .build());
    }

    public static <T> T await(Mono<T> mono) {
        return mono.block(TIMEOUT);
    }

    public static <T> Mono<T> withConnection(Function<Connection, Mono<T>> work) {
        return Mono.usingWhen(CONNECTION_FACTORY.create(), work, Connection::close);
    }

    /**
     * Executes a query and counts its returned rows.
     */
    public static Mono<Long> execute(Connection connection, String sql, Object... arguments) {
        var statement = connection.createStatement(sql);
        for (int i = 0; i < arguments.length; i++) {
            statement.bind(i, arguments[i]);
        }
        return Flux.from(statement.execute())
                .concatMap(result -> result.map((row, metadata) -> 1L))
                .count();
    }

    /**
     * Terminates matching backends and returns the number successfully terminated.
     */
    public static long terminateConnections(String applicationName) {
        return await(withConnection(connection ->
                Mono.from(connection.createStatement(SELECT_TERMINATED_SQL)
                                .bind(0, applicationName)
                                .execute())
                        .flatMap(result -> Mono.from(
                                result.map((row, metadata) -> row.get(0, Long.class))))));
    }
}
