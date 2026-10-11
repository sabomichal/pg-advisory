package com.github.sabomichal.pgadvisory.r2dbc;

import io.r2dbc.spi.Connection;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Optional;

/** All SQL of the library: the lock table and the advisory lock functions. */
interface LockStore {

    String tableName();

    /** Fails when the lock table is missing and {@code autoCreate} is off. */
    Mono<Void> ensureTable(Connection connection, boolean autoCreate);

    Mono<Void> setApplicationName(Connection connection, String applicationName);

    /** Runs {@code work} in a transaction: commits on success, rolls back on error. */
    <T> Mono<T> inTransaction(Connection connection, Mono<T> work);

    /** Id of the lock table row of {@code key}, inserting the row when missing. */
    Mono<Long> findOrCreateRow(Connection connection, String key);

    Mono<Boolean> tryLock(Connection connection, long lockId);

    /** A round trip; fails when the connection's backend is gone. */
    Mono<Void> ping(Connection connection);

    Mono<Boolean> unlock(Connection connection, long lockId);

    Mono<Optional<String>> owner(Connection connection, long rowId);

    /** Increments the generation and records {@code owner} as the holder. */
    Mono<NextGeneration> nextGeneration(Connection connection, long rowId, String owner);

    /** Bounds every lock wait of the current transaction; a longer wait fails with {@code lock_not_available}. */
    Mono<Void> setLockTimeout(Connection connection, Duration timeout);

    /** Takes the exclusive barrier lock until the end of the current transaction. */
    Mono<Void> takeExclusiveBarrier(Connection connection, int namespace, int key);

    /** Takes the shared barrier lock until the end of the current transaction. */
    Mono<Void> takeSharedBarrier(Connection connection, int namespace, int key);

    /** Empty when the row does not exist. */
    Mono<GenerationRead> readGeneration(Connection connection, long rowId);

    /** Locking read; fails with a serialization error when the row changed after the transaction's snapshot. */
    Mono<Long> readGenerationForShare(Connection connection, long rowId);

    record NextGeneration(long generation, @Nullable String previousOwner) {
    }

    record GenerationRead(long generation, boolean readCommitted) {
    }
}
