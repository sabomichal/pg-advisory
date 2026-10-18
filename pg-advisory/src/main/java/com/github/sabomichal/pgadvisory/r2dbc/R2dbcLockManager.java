package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link LockManager} on one shared R2DBC connection, which serves one operation at a time, in the order of submission.
 * <p>
 * A key is locked in two places: in memory, so that the same instance cannot lock it twice (session advisory locks
 * are reentrant), and in PostgreSQL by {@code pg_try_advisory_lock} on the id of the key's row in the lock table.
 */
public final class R2dbcLockManager implements LockManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(R2dbcLockManager.class);

    private final ConnectionFactory lockConnectionFactory;
    private final LockManagerSettings settings;
    private final List<LockManagerListener> listeners;
    private final AdvisoryLockDao dao;

    private final Set<String> currentlyProcessingLocks = ConcurrentHashMap.newKeySet();
    private final SerialExecutor executor = new SerialExecutor();

    private volatile SingleConnectionFactory connectionFactory;

    /**
     * @param lockConnectionFactory unpooled factory of the lock connection
     * @param listeners             called in order, a failing one is logged and does not stop the others
     */
    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings,
                            List<? extends LockManagerListener> listeners) {
        this.lockConnectionFactory = lockConnectionFactory;
        this.settings = settings;
        this.listeners = List.copyOf(listeners);
        this.dao = new AdvisoryLockDao(settings.tableName());
        this.connectionFactory = newConnectionFactory();
    }

    /** Without listeners. */
    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings) {
        this(lockConnectionFactory, settings, List.of());
    }

    /**
     * Waits up to the shutdown lock timeout for the queued operations, then closes the lock connection anyway, so that
     * its locks are released. Waits even when interrupted, the thread stays interrupted.
     */
    @Override
    public void close() {
        // a lambda, not a method reference: the connection factory may be re-created before the task runs
        executor.submit(() -> connectionFactory.destroy())
                .timeout(settings.shutdownLockTimeout(), Mono.defer(() -> {
                    log.warn("[{}] Operations on the lock connection did not finish within [{}], closing it anyway",
                            settings.applicationName(), settings.shutdownLockTimeout());
                    return connectionFactory.destroy();
                }))
                .toFuture()
                // unlike block(), join() keeps waiting when interrupted
                .join();
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
                            case LockAcquisition.Locked(var handle, _) -> Mono.defer(() -> work.apply(handle))
                                    .contextWrite(context -> context.put(LockHandle.CONTEXT_KEY, handle))
                                    .doFirst(() -> onLock.accept(handle))
                                    .doFinally(_ -> onFinally.accept(handle))
                                    .<LockOutcome<T>>map(LockOutcome.Completed::new);
                            case LockAcquisition.Busy(var sameInstance) -> Mono.just(new LockOutcome.Busy<T>(sameInstance));
                        },
                        lockAcquisition -> lockAcquisition instanceof LockAcquisition.Locked locked ? unlockInternal(locked) : Mono.empty())
                .doOnError(error -> {
                    if (rootCause(error) instanceof R2dbcNonTransientResourceException) {
                        validateDbConnection().subscribe();
                    }
                });
    }

    @Override
    public Mono<Void> validateDbConnection() {
        return executor.submit(() -> connectionFactory.create()
                .flatMap(connection -> Mono.from(connection.validate(ValidationDepth.REMOTE)))
                .flatMap(valid -> valid ? Mono.<Void>empty() : Mono.error(new DbConnectionValidationException()))
                .doOnError(R2dbcNonTransientResourceException.class, _ -> reinitDbConnection())
                .doOnError(DbConnectionValidationException.class, _ -> reinitDbConnection())
                .onErrorResume(R2dbcNonTransientResourceException.class, _ -> Mono.empty())
                .onErrorResume(DbConnectionValidationException.class, _ -> Mono.empty()));
    }

    /** Verifies the lock table, creates a missing one when {@link LockManagerSettings#autoCreateSchema()}. */
    public Mono<Void> ensureTable() {
        return executor.submit(() -> connectionFactory.create()
                .flatMap(connection -> dao.ensureTable(connection, settings.autoCreateSchema())));
    }

    private Mono<LockAcquisition> lock(String key) {
        var started = System.nanoTime();
        if (!currentlyProcessingLocks.add(key)) {
            log.debug("[{}] Busy key=[{}], held by this instance", settings.applicationName(), key);
            notifyListeners(listener -> listener.onBusy(key, new LockOutcome.Busy<>(true), since(started)));
            return Mono.just(new LockAcquisition.Busy(true));
        }
        return executor.submit(() -> connectionFactory.create()
                .flatMap(connection -> Mono.from(connection.beginTransaction())
                        .then(dao.createLockRecord(connection, key))
                        .doOnNext(rowsUpdated -> log.trace(rowsUpdated > 0
                                ? "[{}] Lock record created key=[{}]"
                                : "[{}] Lock record exists key=[{}]", settings.applicationName(), key))
                        .then(dao.lock(connection, key))
                        .flatMap(locked -> locked
                                ? commitLocked(connection, key, started)
                                : rollbackBusy(connection, key, started))
                        .onErrorResume(error -> {
                            log.trace("[{}] Rollback key=[{}]", settings.applicationName(), key);
                            return Mono.from(connection.rollbackTransaction()).then(Mono.error(error));
                        }))
                .doOnError(error -> {
                    currentlyProcessingLocks.remove(key);
                    log.debug("[{}] Lock failed key=[{}]", settings.applicationName(), key, error);
                })
                .onErrorMap(error -> new LockFailedException("Could not lock key=[" + key + "]", error))
                .doOnError(LockFailedException.class,
                        error -> notifyListeners(listener -> listener.onLockFailed(key, error, since(started)))));
    }

    private Mono<LockAcquisition> commitLocked(Connection connection, String key, long started) {
        log.trace("[{}] Commit key=[{}]", settings.applicationName(), key);
        return Mono.from(connection.commitTransaction())
                .then(Mono.fromSupplier(() -> {
                    var locked = new LockAcquisition.Locked(new DefaultLockHandle(key), System.nanoTime());
                    log.debug("[{}] Locked key=[{}]", settings.applicationName(), key);
                    notifyListeners(listener -> listener.onLocked(locked.handle(), Duration.ofNanos(locked.lockedNanos() - started)));
                    return locked;
                }));
    }

    private Mono<LockAcquisition> rollbackBusy(Connection connection, String key, long started) {
        log.trace("[{}] Rollback key=[{}]", settings.applicationName(), key);
        return Mono.from(connection.rollbackTransaction())
                .<LockAcquisition>thenReturn(new LockAcquisition.Busy(false))
                .doOnNext(_ -> {
                    currentlyProcessingLocks.remove(key);
                    log.debug("[{}] Busy key=[{}], held by another instance", settings.applicationName(), key);
                    notifyListeners(listener -> listener.onBusy(key, new LockOutcome.Busy<>(false), since(started)));
                });
    }

    private Mono<String> unlockInternal(LockAcquisition.Locked locked) {
        var key = locked.handle().key();
        return executor.submit(() -> connectionFactory.create()
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
                .doOnError(error -> log.warn("[{}] Unlock failed key=[{}], it stays busy on this instance "
                        + "until the lock connection is re-created", settings.applicationName(), key, error))
                .doOnTerminate(() -> notifyListeners(listener -> listener.onReleased(locked.handle(), since(locked.lockedNanos())))));
    }

    private void reinitDbConnection() {
        log.warn("[{}] Lock connection is broken, re-creating it, all its locks are lost", settings.applicationName());
        notifyListeners(LockManagerListener::onConnectionReset);
        connectionFactory.destroy().subscribe();
        connectionFactory = newConnectionFactory();
    }

    private SingleConnectionFactory newConnectionFactory() {
        currentlyProcessingLocks.clear();
        return new SingleConnectionFactory(lockConnectionFactory,
                connection -> dao.setApplicationName(connection, settings.applicationName()));
    }

    private void notifyListeners(Consumer<LockManagerListener> callback) {
        for (var listener : listeners) {
            try {
                callback.accept(listener);
            } catch (RuntimeException e) {
                log.warn("[{}] Listener [{}] failed", settings.applicationName(), listener, e);
            }
        }
    }

    private static Duration since(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos);
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

        /** {@code lockedNanos} by {@link System#nanoTime()}, for the hold time. */
        record Locked(LockHandle handle, long lockedNanos) implements LockAcquisition {
        }

        record Busy(boolean sameInstance) implements LockAcquisition {
        }
    }

    /** The lock connection failed its validation. */
    public static class DbConnectionValidationException extends RuntimeException {
    }
}
