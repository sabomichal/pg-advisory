package com.github.sabomichal.pgadvisory.r2dbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * SQL of the lock table: one row per key, whose {@code id} is the advisory lock id.
 */
final class AdvisoryLockDao {

    private static final String TABLE_EXISTS = "SELECT to_regclass($1) IS NOT NULL";

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS %s (
                id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                lock_key text   NOT NULL UNIQUE
            )""";

    private static final String SET_APPLICATION_NAME = "SELECT set_config('application_name', $1, false)";

    private static final String CREATE_LOCK_RECORD = "INSERT INTO %s (lock_key) VALUES ($1) ON CONFLICT (lock_key) DO NOTHING";

    private static final String LOCK = "SELECT pg_try_advisory_lock(id) AS lock_result FROM %s WHERE lock_key = $1";

    private static final String UNLOCK = "SELECT pg_advisory_unlock(id) AS unlock_result FROM %s WHERE lock_key = $1";

    private final String tableName;

    AdvisoryLockDao(String tableName) {
        this.tableName = tableName;
    }

    Mono<Void> ensureTable(Connection connection, boolean autoCreate) {
        return queryOne(connection, TABLE_EXISTS, row -> row.get(0, Boolean.class), tableName)
                .flatMap(exists -> {
                    if (exists) {
                        return Mono.empty();
                    }
                    if (!autoCreate) {
                        return Mono.error(new IllegalStateException(("Advisory lock table [%s] does not exist. Create it "
                                + "(see classpath:db/pg-advisory/V1__advisory_lock.sql) or enable spring.pg-advisory.schema.auto-create")
                                .formatted(tableName)));
                    }
                    return Flux.from(connection.createStatement(CREATE_TABLE.formatted(tableName)).execute())
                            .concatMap(Result::getRowsUpdated)
                            .then();
                });
    }

    Mono<Void> setApplicationName(Connection connection, String applicationName) {
        return queryOne(connection, SET_APPLICATION_NAME, _ -> 1, applicationName).then();
    }

    /** @return the number of inserted rows, 0 when the key's row already exists */
    Mono<Long> createLockRecord(Connection connection, String key) {
        return Flux.from(connection.createStatement(CREATE_LOCK_RECORD.formatted(tableName)).bind(0, key).execute())
                .concatMap(Result::getRowsUpdated)
                .reduce(0L, Long::sum);
    }

    Mono<Boolean> lock(Connection connection, String key) {
        return queryOne(connection, LOCK.formatted(tableName), row -> row.get("lock_result", Boolean.class), key);
    }

    Mono<Boolean> unlock(Connection connection, String key) {
        return queryOne(connection, UNLOCK.formatted(tableName), row -> row.get("unlock_result", Boolean.class), key);
    }

    private static <T> Mono<T> queryOne(Connection connection, String sql, Function<Readable, T> mapper, Object... arguments) {
        var statement = connection.createStatement(sql);
        for (var i = 0; i < arguments.length; i++) {
            statement.bind(i, arguments[i]);
        }
        return Flux.from(statement.execute())
                .concatMap(result -> result.map(mapper))
                .next();
    }
}
