package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.ConnectionResetListener;
import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link LockManager} on one shared R2DBC connection, which serves one operation at a time.
 * <p>
 * A key is locked in two places: in memory, so that the same instance cannot lock it twice (session advisory locks
 * are reentrant), and in PostgreSQL by {@code pg_try_advisory_lock} on the id of the key's row in the lock table.
 */
public final class R2dbcLockManager implements LockManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(R2dbcLockManager.class);

    private final ConnectionFactory lockConnectionFactory;
    private final LockManagerSettings settings;
    private final ConnectionResetListener connectionResetListener;
    private final AdvisoryLockDao dao;
    private final Retry lockRetrySpec;

    private final Set<String> currentlyProcessingLocks = ConcurrentHashMap.newKeySet();
    private final Lock lock = new StampedLock().asWriteLock();

    private volatile SingleConnectionFactory connectionFactory;

    /**
     * @param lockConnectionFactory   unpooled factory of the lock connection
     * @param connectionResetListener called when the lock connection is re-created and its locks are gone
     */
    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings,
                            ConnectionResetListener connectionResetListener) {
        this.lockConnectionFactory = lockConnectionFactory;
        this.settings = settings;
        this.connectionResetListener = connectionResetListener;
        this.dao = new AdvisoryLockDao(settings.tableName());
        this.lockRetrySpec = Retry.backoff(settings.retryMaxAttempts(), settings.retryMinBackoff())
                .maxBackoff(settings.retryMaxBackoff())
                .jitter(settings.retryJitter())
                .filter(error -> error instanceof LockAcquisitionException)
                .onRetryExhaustedThrow((_, _) -> new IllegalStateException("LockManager was unable to obtain lock"));
        this.connectionFactory = newConnectionFactory();
    }

    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings) {
        this(lockConnectionFactory, settings, () -> {});
    }

    /**
     * Waits for the running operation and closes the lock connection even when interrupted, so that its locks are
     * released. The thread stays interrupted.
     */
    @Override
    public void close() {
        if (!tryLockUninterruptibly(settings.shutdownLockTimeout())) {
            throw new LockAcquisitionException();
        }
        try {
            // unlike block(), join() keeps waiting when interrupted
            connectionFactory.destroy().toFuture().join();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public <T> Mono<LockOutcome<T>> tryWithLock(String key, Function<LockHandle, Mono<T>> work) {
        return tryWithLock(key, _ -> {}, _ -> {}, work);
    }

    @Override
    public <T> Mono<LockOutcome<T>> tryWithLock(
            String key, Consumer<LockHandle> onLock, Consumer<LockHandle> onFinally, Function<LockHandle, Mono<T>> work) {
        return Mono.usingWhen(
                        Mono.defer(() -> lock(key)),
                        lockAcquisition -> switch (lockAcquisition) {
                            case LockAcquisition.Locked(var handle) -> Mono.defer(() -> work.apply(handle))
                                    .contextWrite(context -> context.put(LockHandle.CONTEXT_KEY, handle))
                                    .doFirst(() -> onLock.accept(handle))
                                    .doFinally(_ -> onFinally.accept(handle))
                                    .<LockOutcome<T>>map(LockOutcome.Completed::new);
                            case LockAcquisition.Busy(var sameInstance) -> Mono.just(new LockOutcome.Busy<T>(sameInstance));
                        },
                        lockAcquisition -> lockAcquisition instanceof LockAcquisition.Locked ? unlockInternal(key) : Mono.empty())
                .doOnError(error -> {
                    if (rootCause(error) instanceof R2dbcNonTransientResourceException) {
                        validateDbConnection().subscribe();
                    }
                });
    }

    @Override
    public Mono<Void> validateDbConnection() {
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> Mono.from(connection.validate(ValidationDepth.REMOTE)))
                        .flatMap(valid -> valid ? Mono.<Void>empty() : Mono.error(new DbConnectionValidationException()))
                        .doOnError(R2dbcNonTransientResourceException.class, _ -> reinitDbConnection())
                        .doOnError(DbConnectionValidationException.class, _ -> reinitDbConnection())
                        .onErrorResume(R2dbcNonTransientResourceException.class, _ -> Mono.empty())
                        .onErrorResume(DbConnectionValidationException.class, _ -> Mono.empty())
                        .doFinally(_ -> lock.unlock())));
    }

    /** Verifies the lock table, creates a missing one when {@link LockManagerSettings#autoCreateSchema()}. */
    public Mono<Void> ensureTable() {
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> dao.ensureTable(connection, settings.autoCreateSchema()))
                        .doFinally(_ -> lock.unlock())));
    }

    private Mono<LockAcquisition> lock(String key) {
        if (!currentlyProcessingLocks.add(key)) {
            log.debug("[{}] Busy key=[{}], held by this instance", settings.applicationName(), key);
            return Mono.just(new LockAcquisition.Busy(true));
        }
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> Mono.from(connection.beginTransaction())
                                .then(dao.createLockRecord(connection, key))
                                .doOnNext(rowsUpdated -> log.trace(rowsUpdated > 0
                                        ? "[{}] Lock record created key=[{}]"
                                        : "[{}] Lock record exists key=[{}]", settings.applicationName(), key))
                                .then(dao.lock(connection, key))
                                .flatMap(locked -> locked
                                        ? commitLocked(connection, key)
                                        : rollbackBusy(connection, key))
                                .onErrorResume(error -> {
                                    log.trace("[{}] Rollback key=[{}]", settings.applicationName(), key);
                                    return Mono.from(connection.rollbackTransaction()).then(Mono.error(error));
                                }))
                        .doFinally(_ -> lock.unlock())))
                .doOnError(error -> {
                    currentlyProcessingLocks.remove(key);
                    log.debug("[{}] Lock failed key=[{}]", settings.applicationName(), key, error);
                })
                .onErrorMap(error -> new LockFailedException("Could not lock key=[" + key + "]", error));
    }

    private Mono<LockAcquisition> commitLocked(Connection connection, String key) {
        log.trace("[{}] Commit key=[{}]", settings.applicationName(), key);
        return Mono.from(connection.commitTransaction())
                .<LockAcquisition>thenReturn(new LockAcquisition.Locked(new DefaultLockHandle(key)))
                .doOnNext(_ -> log.debug("[{}] Locked key=[{}]", settings.applicationName(), key));
    }

    private Mono<LockAcquisition> rollbackBusy(Connection connection, String key) {
        log.trace("[{}] Rollback key=[{}]", settings.applicationName(), key);
        return Mono.from(connection.rollbackTransaction())
                .<LockAcquisition>thenReturn(new LockAcquisition.Busy(false))
                .doOnNext(_ -> {
                    currentlyProcessingLocks.remove(key);
                    log.debug("[{}] Busy key=[{}], held by another instance", settings.applicationName(), key);
                });
    }

    private Mono<String> unlockInternal(String key) {
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> dao.unlock(connection, key))
                        .map(unlocked -> {
                            if (unlocked) {
                                log.debug("[{}] Unlocked key=[{}]", settings.applicationName(), key);
                            } else {
                                log.warn("[{}] Unlock found key=[{}] already released, the lock connection was "
                                        + "probably re-created", settings.applicationName(), key);
                            }
                            return key;
                        })
                        .doOnSuccess(_ -> currentlyProcessingLocks.remove(key))
                        .doFinally(_ -> lock.unlock())))
                .doOnError(error -> log.warn("[{}] Unlock failed key=[{}], it stays busy on this instance "
                        + "until the lock connection is re-created", settings.applicationName(), key, error));
    }

    private void reinitDbConnection() {
        log.warn("[{}] Lock connection is broken, re-creating it, all its locks are lost", settings.applicationName());
        connectionResetListener.onConnectionReset();
        connectionFactory.destroy().subscribe();
        connectionFactory = newConnectionFactory();
    }

    private SingleConnectionFactory newConnectionFactory() {
        currentlyProcessingLocks.clear();
        return new SingleConnectionFactory(lockConnectionFactory,
                connection -> dao.setApplicationName(connection, settings.applicationName()));
    }

    /** The lock connection serves one operation at a time: waits for it with backoff, see the retry settings. */
    private Mono<Boolean> obtainLock() {
        return Mono.fromSupplier(() -> {
            if (lock.tryLock()) {
                return true;
            }
            throw new LockAcquisitionException();
        }).retryWhen(lockRetrySpec);
    }

    private boolean tryLockUninterruptibly(Duration timeout) {
        var deadline = System.nanoTime() + timeout.toNanos();
        var interrupted = false;
        try {
            while (true) {
                try {
                    return lock.tryLock(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
                } catch (InterruptedException _) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static Throwable rootCause(Throwable error) {
        var cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    /** What {@link #lock} got: the lock, or a key locked by this or another instance. */
    private sealed interface LockAcquisition {

        record Locked(LockHandle handle) implements LockAcquisition {
        }

        record Busy(boolean sameInstance) implements LockAcquisition {
        }
    }

    /** The lock connection is busy with another operation. */
    public static class LockAcquisitionException extends RuntimeException {
    }

    /** The lock connection failed its validation. */
    public static class DbConnectionValidationException extends RuntimeException {
    }
}
