package com.github.sabomichal.pgadvisory.r2dbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Readable;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

final class R2dbcLockStore implements LockStore {

    private static final String TABLE_EXISTS = "SELECT to_regclass($1) IS NOT NULL";

    private static final String CREATE_TABLE = """
            CREATE TABLE IF NOT EXISTS %s (
                id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                lock_key   text   NOT NULL UNIQUE,
                generation bigint NOT NULL DEFAULT 0,
                owner      text
            )""";

    private static final String SET_APPLICATION_NAME = "SELECT set_config('application_name', $1, false)";

    private static final String SELECT_ROW_ID = "SELECT id FROM %s WHERE lock_key = $1";

    private static final String INSERT_ROW = "INSERT INTO %s (lock_key) VALUES ($1) ON CONFLICT (lock_key) DO NOTHING RETURNING id";

    private static final String TRY_LOCK = "SELECT pg_try_advisory_lock($1)";

    private static final String UNLOCK = "SELECT pg_advisory_unlock($1)";

    private static final String PING = "SELECT 1";

    private static final String SELECT_OWNER = "SELECT owner FROM %s WHERE id = $1";

    private static final String NEXT_GENERATION = """
            WITH previous AS (SELECT owner FROM %1$s WHERE id = $1)
            UPDATE %1$s SET generation = generation + 1, owner = $2 WHERE id = $1
            RETURNING generation, (SELECT owner FROM previous)""";

    private static final String SET_LOCK_TIMEOUT = "SELECT set_config('lock_timeout', $1, true)";

    private static final String EXCLUSIVE_BARRIER = "SELECT 1 FROM pg_advisory_xact_lock($1, $2)";

    private static final String SHARED_BARRIER = "SELECT 1 FROM pg_advisory_xact_lock_shared($1, $2)";

    private static final String SELECT_GENERATION = """
            SELECT generation, current_setting('transaction_isolation') = 'read committed'
            FROM %s WHERE id = $1""";

    private static final String SELECT_GENERATION_FOR_SHARE = "SELECT generation FROM %s WHERE id = $1 FOR SHARE";

    private final String tableName;

    R2dbcLockStore(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public String tableName() {
        return tableName;
    }

    @Override
    public Mono<Void> ensureTable(Connection connection, boolean autoCreate) {
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
                    return execute(connection, CREATE_TABLE.formatted(tableName));
                });
    }

    @Override
    public Mono<Void> setApplicationName(Connection connection, String applicationName) {
        return queryOne(connection, SET_APPLICATION_NAME, _ -> 1, applicationName).then();
    }

    @Override
    public <T> Mono<T> inTransaction(Connection connection, Mono<T> work) {
        return Mono.from(connection.beginTransaction())
                .then(work)
                .flatMap(result -> Mono.from(connection.commitTransaction()).thenReturn(result))
                .switchIfEmpty(Mono.defer(() -> Mono.from(connection.commitTransaction()).then(Mono.empty())))
                .onErrorResume(error -> Mono.from(connection.rollbackTransaction())
                        .onErrorComplete()
                        .then(Mono.error(error)));
    }

    @Override
    public Mono<Long> findOrCreateRow(Connection connection, String key) {
        Function<Readable, Long> id = row -> row.get(0, Long.class);
        return queryOne(connection, SELECT_ROW_ID.formatted(tableName), id, key)
                .switchIfEmpty(queryOne(connection, INSERT_ROW.formatted(tableName), id, key))
                // the insert lost a race with another instance inserting the same key
                .switchIfEmpty(queryOne(connection, SELECT_ROW_ID.formatted(tableName), id, key));
    }

    @Override
    public Mono<Boolean> tryLock(Connection connection, long lockId) {
        return queryOne(connection, TRY_LOCK, row -> row.get(0, Boolean.class), lockId);
    }

    @Override
    public Mono<Void> ping(Connection connection) {
        return queryOne(connection, PING, _ -> 1).then();
    }

    @Override
    public Mono<Boolean> unlock(Connection connection, long lockId) {
        return queryOne(connection, UNLOCK, row -> row.get(0, Boolean.class), lockId);
    }

    @Override
    public Mono<Optional<String>> owner(Connection connection, long rowId) {
        return queryOne(connection, SELECT_OWNER.formatted(tableName), row -> Optional.ofNullable(row.get(0, String.class)), rowId)
                .defaultIfEmpty(Optional.empty());
    }

    @Override
    public Mono<NextGeneration> nextGeneration(Connection connection, long rowId, String owner) {
        return queryOne(connection, NEXT_GENERATION.formatted(tableName),
                row -> new NextGeneration(row.get(0, Long.class), row.get(1, String.class)), rowId, owner);
    }

    @Override
    public Mono<Void> setLockTimeout(Connection connection, Duration timeout) {
        return queryOne(connection, SET_LOCK_TIMEOUT, _ -> 1, timeout.toMillis() + "ms").then();
    }

    @Override
    public Mono<Void> takeExclusiveBarrier(Connection connection, int namespace, int key) {
        return queryOne(connection, EXCLUSIVE_BARRIER, _ -> 1, namespace, key).then();
    }

    @Override
    public Mono<Void> takeSharedBarrier(Connection connection, int namespace, int key) {
        return queryOne(connection, SHARED_BARRIER, _ -> 1, namespace, key).then();
    }

    @Override
    public Mono<GenerationRead> readGeneration(Connection connection, long rowId) {
        return queryOne(connection, SELECT_GENERATION.formatted(tableName),
                row -> new GenerationRead(row.get(0, Long.class), row.get(1, Boolean.class)), rowId);
    }

    @Override
    public Mono<Long> readGenerationForShare(Connection connection, long rowId) {
        return queryOne(connection, SELECT_GENERATION_FOR_SHARE.formatted(tableName), row -> row.get(0, Long.class), rowId);
    }

    private static <T> Mono<T> queryOne(Connection connection, String sql, Function<Readable, T> mapper, Object... arguments) {
        return Flux.from(bind(connection.createStatement(sql), arguments).execute())
                .concatMap(result -> result.map(mapper))
                .next();
    }

    private static Mono<Void> execute(Connection connection, String sql) {
        return Flux.from(connection.createStatement(sql).execute())
                .concatMap(Result::getRowsUpdated)
                .then();
    }

    private static Statement bind(Statement statement, Object... arguments) {
        for (var i = 0; i < arguments.length; i++) {
            statement.bind(i, arguments[i]);
        }
        return statement;
    }
}
