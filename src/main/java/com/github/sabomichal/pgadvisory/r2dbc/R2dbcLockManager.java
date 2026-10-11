package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.ConnectionResetListener;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.LockOperationException;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

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

        if (!tryLock(settings.initialLockTimeout().toMillis())) {
            throw new LockAcquisitionException();
        }
        try {
            this.connectionFactory = newConnectionFactory();
        } finally {
            lock.unlock();
        }
    }

    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings) {
        this(lockConnectionFactory, settings, () -> {
        });
    }

    @Override
    public void close() {
        if (!tryLock(settings.shutdownLockTimeout().toMillis())) {
            throw new LockAcquisitionException();
        }
        try {
            connectionFactory.destroy().block();
        } finally {
            lock.unlock();
        }
    }

    /** Verifies the lock table; creates it when it is missing and {@link LockManagerSettings#autoCreateSchema()}. */
    public Mono<Void> ensureTable() {
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> dao.ensureTable(connection, settings.autoCreateSchema()))
                        .doFinally(_ -> lock.unlock())));
    }

    @Override
    public <T> Mono<T> tryWithLock(String key, Function<String, Mono<T>> function) {
        return tryWithLock(key, _ -> {
        }, _ -> {
        }, function);
    }

    @Override
    public <T> Mono<T> tryWithLock(String key, Consumer<String> onLock, Consumer<String> onFinally, Function<String, Mono<T>> function) {
        log.debug("Acquiring pg session advisory lock for key=[{}]", key);
        return Mono.usingWhen(
                        Mono.defer(() -> lock(key)),
                        lockedKey -> Mono.defer(() -> function.apply(lockedKey))
                                .doFirst(() -> onLock.accept(lockedKey))
                                .doFinally(_ -> onFinally.accept(lockedKey)),
                        lockedKey -> {
                            log.debug("Releasing pg session advisory lock for key=[{}]", lockedKey);
                            return unlockInternal(lockedKey);
                        })
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

    private Mono<String> lock(String key) {
        addToCurrentlyProcessingLocks(key);
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> Mono.from(connection.beginTransaction())
                                .then(dao.createLockRecord(connection, key))
                                .doOnNext(rowsUpdated -> {
                                    if (rowsUpdated > 0) {
                                        log.trace("DB lock record created [{}] - {}", settings.applicationName(), key);
                                    } else {
                                        log.trace("DB lock record already exists [{}] - {}", settings.applicationName(), key);
                                    }
                                })
                                .then(dao.lock(connection, key))
                                .flatMap(locked -> {
                                    if (!locked) {
                                        log.trace("DB lock error [{}] - {}", settings.applicationName(), key);
                                        return Mono.error(new LockOperationException("Could not acquire lock for key=[" + key + "]"));
                                    }
                                    log.trace("DB lock OK [{}] - {}", settings.applicationName(), key);
                                    log.trace("DB commit [{}] - {}", settings.applicationName(), key);
                                    return Mono.from(connection.commitTransaction()).thenReturn(key);
                                })
                                .onErrorResume(error -> {
                                    log.trace("DB rollback [{}] - {}", settings.applicationName(), key);
                                    return Mono.from(connection.rollbackTransaction()).then(Mono.error(error));
                                }))
                        .doFinally(_ -> lock.unlock())))
                .doOnError(_ -> removeFromCurrentlyProcessingLocks(key));
    }

    private Mono<String> unlockInternal(String key) {
        return obtainLock()
                .then(Mono.defer(() -> connectionFactory.create()
                        .flatMap(connection -> dao.unlock(connection, key))
                        .map(unlocked -> {
                            if (!unlocked) {
                                log.warn("Could not release lock for key=[{}] - already released", key);
                            } else {
                                log.trace("DB lock released [{}] - {}", settings.applicationName(), key);
                            }
                            return key;
                        })
                        .doOnSuccess(_ -> removeFromCurrentlyProcessingLocks(key))
                        .doFinally(_ -> lock.unlock())));
    }

    private void reinitDbConnection() {
        log.warn("LockManager DB connection is invalid, reinitializing");
        connectionResetListener.onConnectionReset();
        connectionFactory.destroy().subscribe();
        connectionFactory = newConnectionFactory();
    }

    private SingleConnectionFactory newConnectionFactory() {
        currentlyProcessingLocks.clear();
        return new SingleConnectionFactory(lockConnectionFactory,
                connection -> dao.setApplicationName(connection, settings.applicationName()));
    }

    private void addToCurrentlyProcessingLocks(String key) {
        if (!currentlyProcessingLocks.add(key)) {
            log.trace("MEM lock error [{}] - {}", settings.applicationName(), key);
            throw new LockOperationException("Could not acquire lock for key=[" + key + "]", true);
        }
        log.trace("MEM lock OK [{}] - {}", settings.applicationName(), key);
    }

    private void removeFromCurrentlyProcessingLocks(String key) {
        log.trace("MEM lock released [{}] - {}", settings.applicationName(), key);
        currentlyProcessingLocks.remove(key);
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

    private boolean tryLock(long timeoutMillis) {
        try {
            return lock.tryLock(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    private static Throwable rootCause(Throwable error) {
        var cause = error;
        while (cause.getCause() != null && cause.getCause() != cause) {
            cause = cause.getCause();
        }
        return cause;
    }

    /** The lock connection is busy with another operation. */
    public static class LockAcquisitionException extends RuntimeException {
    }

    /** The lock connection failed its validation. */
    public static class DbConnectionValidationException extends RuntimeException {
    }
}
