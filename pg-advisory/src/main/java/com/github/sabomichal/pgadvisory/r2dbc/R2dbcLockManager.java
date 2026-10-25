package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link LockManager} on one shared R2DBC connection, which serves one operation at a time, in the order of submission.
 * <p>
 * A key is locked in two places: in memory, so that the same instance cannot lock it twice (session advisory locks
 * are reentrant), and in PostgreSQL by {@code pg_try_advisory_lock} on the id of the key's row in the lock table.
 * <p>
 * The lock connection, its queue and its heartbeat form a session. The heartbeat checks the connection every
 * {@link LockManagerSettings#heartbeatInterval()}, and a broken session is replaced by a new one. Operations still queued
 * on the broken one end with it: a lock fails, an unlock has nothing left to release.
 */
public final class R2dbcLockManager implements LockManager, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(R2dbcLockManager.class);

    private final ConnectionFactory lockConnectionFactory;
    private final LockManagerSettings settings;
    private final List<LockManagerListener> listeners;
    private final AdvisoryLockDao dao;

    private final Set<String> currentlyProcessingLocks = ConcurrentHashMap.newKeySet();
    private final AtomicBoolean closed = new AtomicBoolean();

    private volatile LockSession session;

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
        this.session = createSession();
    }

    /**
     * Without listeners
     */
    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings) {
        this(lockConnectionFactory, settings, List.of());
    }

    /**
     * Waits up to the shutdown lock timeout for the queued operations, then closes the lock connection anyway, so that
     * its locks are released. Waits even when interrupted, the thread stays interrupted.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        session.close()
                .toFuture()
                // unlike block(), join() keeps waiting when interrupted
                .join();
    }

    @Override
    public <T> Mono<LockOutcome<T>> tryWithLock(String key, Function<LockHandle, Mono<T>> work) {
        return tryWithLock(
                key, _ -> {
                }, _ -> {
                }, work
        );
    }

    @Override
    public <T> Mono<LockOutcome<T>> tryWithLock(
            String key, Consumer<LockHandle> onLock, Consumer<LockHandle> onFinally, Function<LockHandle, Mono<T>> work) {
        return Mono.usingWhen(
                        Mono.defer(() -> lock(key)),
                        lockAcquisition -> switch (lockAcquisition) {
                            case LockAcquisition.Locked(var handle, _, _) -> Mono.defer(() -> work.apply(handle))
                                    .contextWrite(context -> context.put(LockHandle.CONTEXT_KEY, handle))
                                    .doFirst(() -> onLock.accept(handle))
                                    .doFinally(_ -> onFinally.accept(handle))
                                    .<LockOutcome<T>>map(LockOutcome.Completed::new);
                            case LockAcquisition.Busy(var sameInstance) -> Mono.just(new LockOutcome.Busy<T>(sameInstance));
                        },
                        lockAcquisition -> lockAcquisition instanceof LockAcquisition.Locked locked ? unlockInternal(locked) : Mono.empty())
                .doOnError(this::checkConnectionOnError);
    }

    /**
     * Validates the lock connection of the current session and replaces a broken session, closing its connection drops
     * all its locks. Run after a non-transient error, the heartbeat of the session does the same.
     */
    Mono<Void> checkAndResetConnection() {
        return session.checkAndResetConnection();
    }

    /**
     * Verifies the lock table on a connection of its own, creates a missing one when
     * {@link LockManagerSettings#autoCreateSchema()}. Run it before creating the lock manager, with the same factory.
     */
    public static Mono<Void> ensureTable(ConnectionFactory connectionFactory, LockManagerSettings settings) {
        var dao = new AdvisoryLockDao(settings.tableName());
        return Mono.usingWhen(connectionFactory.create(),
                connection -> dao.ensureTable(connection, settings.autoCreateSchema()),
                Connection::close);
    }

    private Mono<LockAcquisition> lock(String key) {
        var started = System.nanoTime();
        if (closed.get()) {
            var error = new LockFailedException("Could not lock key=[" + key + "], the lock manager is closed",
                    new LockSession.SessionFinishedException());
            notifyListeners(listener -> listener.onLockFailed(key, error, since(started)));
            return Mono.error(error);
        }
        if (!currentlyProcessingLocks.add(key)) {
            log.debug("[{}] Busy key=[{}], held by this instance", settings.applicationName(), key);
            notifyListeners(listener -> listener.onBusy(key, new LockOutcome.Busy<>(true), since(started)));
            return Mono.just(new LockAcquisition.Busy(true));
        }
        // no transaction: a failed COMMIT would roll back the INSERT but keep the session advisory lock, which would then leak
        // until the shared lock connection closes. The single INSERT is atomic on its own, so a transaction would protect nothing.
        var session = this.session;
        return session.execute(lockConnection -> lockConnection
                        .flatMap(connection -> dao.createLockRecord(connection, key)
                                .doOnNext(rowsUpdated ->
                                        log.trace(rowsUpdated > 0 ? "[{}] Lock record created key=[{}]" : "[{}] Lock record exists key=[{}]", settings.applicationName(), key))
                                .then(dao.lock(connection, key))
                                .flatMap(acquired -> acquired
                                        ? nextFencingToken(session, connection, key).map(token -> locked(key, token, started, session))
                                        : Mono.just(busy(key, started))))
                        .doOnError(error -> {
                            currentlyProcessingLocks.remove(key);
                            log.debug("[{}] Lock failed key=[{}]", settings.applicationName(), key, error);
                        })
                        .onErrorMap(error -> new LockFailedException("Could not lock key=[" + key + "]", error))
                        .doOnError(LockFailedException.class, error ->
                                notifyListeners(listener -> listener.onLockFailed(key, error, since(started)))
                        ),
                this::releaseAbandonedLock);
    }

    private void releaseAbandonedLock(LockAcquisition acquisition) {
        if (acquisition instanceof LockAcquisition.Locked locked) {
            log.debug("[{}] Releasing key=[{}], caller cancelled before receiving lock",
                    settings.applicationName(), locked.handle().key());
            unlockInternal(locked).subscribe(null, this::checkConnectionOnError);
        }
    }

    private Mono<Long> nextFencingToken(LockSession session, Connection connection, String key) {
        return dao.nextFencingToken(connection, key)
                .onErrorResume(error -> dao.unlock(connection, key)
                        .doOnError(_ -> session.reset())
                        .onErrorComplete()
                        .then(Mono.error(error)));
    }

    private LockAcquisition locked(String key, long fencingToken, long started, LockSession session) {
        var locked = new LockAcquisition.Locked(new DefaultLockHandle(key, fencingToken), System.nanoTime(), session);
        log.debug("[{}] Locked key=[{}] fencingToken=[{}]", settings.applicationName(), key, fencingToken);
        notifyListeners(listener -> listener.onLocked(locked.handle(), Duration.ofNanos(locked.lockedNanos() - started)));
        return locked;
    }

    private LockAcquisition busy(String key, long started) {
        currentlyProcessingLocks.remove(key);
        log.debug("[{}] Busy key=[{}], held by another instance", settings.applicationName(), key);
        notifyListeners(listener -> listener.onBusy(key, new LockOutcome.Busy<>(false), since(started)));
        return new LockAcquisition.Busy(false);
    }

    private Mono<String> unlockInternal(LockAcquisition.Locked locked) {
        var key = locked.handle().key();
        var session = locked.session();
        return session.execute(lockConnection -> lockConnection
                .flatMap(connection -> dao.unlock(connection, key))
                // the lock went with the connection of the ended session
                .onErrorResume(LockSession.SessionFinishedException.class, _ -> Mono.just(false))
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

    private void checkConnectionOnError(Throwable error) {
        if (rootCause(error) instanceof R2dbcNonTransientResourceException) {
            checkAndResetConnection().subscribe();
        }
    }

    private void replaceSession() {
        log.warn("[{}] Lock connection is broken, re-creating it, all its locks are lost", settings.applicationName());
        currentlyProcessingLocks.clear();
        notifyListeners(LockManagerListener::onConnectionReset);
        var nextSession = createSession();
        session = nextSession;
        // close() sets closed before it reads the session, so either it or this closes the new session
        if (closed.get()) {
            nextSession.close().subscribe();
        }
    }

    private LockSession createSession() {
        return new LockSession(lockConnectionFactory, dao, settings, this::replaceSession);
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

    /**
     * What {@link #lock} got: the lock, or a key locked by this or another instance.
     */
    private sealed interface LockAcquisition {

        /**
         * {@code lockedNanos} by {@link System#nanoTime()}, for the hold time. {@code session} holds the lock on its
         * connection, and only it can release it.
         */
        record Locked(LockHandle handle, long lockedNanos, LockSession session) implements LockAcquisition {
        }

        record Busy(boolean sameInstance) implements LockAcquisition {
        }
    }
}
