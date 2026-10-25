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
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * {@link LockManager} on one R2DBC connection, serving one operation at a time in submission order.
 * <p>
 * A key is locked in memory against this instance, since session advisory locks are reentrant, and in PostgreSQL by
 * {@code pg_try_advisory_lock} on the id of its row in the lock table.
 * <p>
 * The connection, its queue and its heartbeat form a session. A session found broken within
 * {@link LockManagerSettings#heartbeatInterval()} is replaced, and the operations queued on it end with it: a lock
 * fails, an unlock has nothing left to release.
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
    public R2dbcLockManager(ConnectionFactory lockConnectionFactory, LockManagerSettings settings, List<? extends LockManagerListener> listeners) {
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
                        attempt -> switch (attempt) {
                            case LockAttempt.Locked(var handle, _, _) -> Mono.defer(() -> work.apply(handle))
                                    .contextWrite(context -> context.put(LockHandle.CONTEXT_KEY, handle))
                                    .doFirst(() -> onLock.accept(handle))
                                    .doFinally(_ -> onFinally.accept(handle))
                                    .<LockOutcome<T>>map(LockOutcome.Completed::new);
                            case LockAttempt.NoToken(_, var error, _) -> Mono.error(error);
                            case LockAttempt.Busy(var sameInstance) -> Mono.just(new LockOutcome.Busy<T>(sameInstance));
                        },
                        attempt -> switch (attempt) {
                            case LockAttempt.Locked locked -> unlock(locked);
                            case LockAttempt.NoToken noToken -> release(noToken.session(), noToken.key());
                            case LockAttempt.Busy _ -> Mono.empty();
                        })
                .doOnError(t -> checkConnectionOnError(session, t));
    }

    /**
     * Closes the lock connection, releasing its locks, after the queued operations or the shutdown lock timeout. Keeps
     * waiting when interrupted, the thread stays interrupted.
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

    /**
     * Verifies the lock table on its own connection, creates a missing one when
     * {@link LockManagerSettings#autoCreateSchema()}. Run it before the lock manager, with the same factory.
     */
    public static Mono<Void> ensureTable(ConnectionFactory connectionFactory, LockManagerSettings settings) {
        var dao = new AdvisoryLockDao(settings.tableName());
        return Mono.usingWhen(connectionFactory.create(),
                connection -> dao.ensureTable(connection, settings.autoCreateSchema()),
                Connection::close);
    }

    private Mono<LockAttempt> lock(String key) {
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
            return Mono.just(new LockAttempt.Busy(true));
        }
        // no transaction: a failed COMMIT would roll back the INSERT but keep the session advisory lock, leaked until the
        // connection closes. The single INSERT is atomic anyway.
        var session = this.session;
        return session.execute(lockConnection -> lockConnection
                        .flatMap(connection -> dao.createLockRecord(connection, key)
                                .doOnNext(rowsUpdated ->
                                        log.trace(rowsUpdated > 0 ? "[{}] Lock record created key=[{}]" : "[{}] Lock record exists key=[{}]", settings.applicationName(), key))
                                .then(dao.lock(connection, key))
                                .flatMap(acquired -> acquired
                                        ? dao.nextFencingToken(connection, key)
                                        .map(token -> locked(key, token, started, session))
                                        // not an error: the held lock is released after this task, like any other
                                        .onErrorResume(error -> Mono.just(noToken(key, error, started, session)))
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

    private void releaseAbandonedLock(LockAttempt attempt) {
        switch (attempt) {
            case LockAttempt.Locked locked -> {
                log.debug("[{}] Releasing key=[{}], caller cancelled before receiving lock", settings.applicationName(), locked.handle().key());
                unlock(locked).subscribe();
            }
            case LockAttempt.NoToken noToken -> {
                release(noToken.session(), noToken.key()).subscribe();
            }
            case LockAttempt.Busy _ -> {
            }
        }
    }

    private Mono<Void> unlock(LockAttempt.Locked locked) {
        return release(locked.session(), locked.handle().key())
                .doOnTerminate(() -> notifyListeners(listener -> listener.onReleased(locked.handle(), since(locked.lockedNanos()))));
    }

    private LockAttempt locked(String key, long fencingToken, long started, LockSession session) {
        var locked = new LockAttempt.Locked(new DefaultLockHandle(key, fencingToken), System.nanoTime(), session);
        log.debug("[{}] Locked key=[{}] fencingToken=[{}]", settings.applicationName(), key, fencingToken);
        notifyListeners(listener -> listener.onLocked(locked.handle(), Duration.ofNanos(locked.lockedNanos() - started)));
        return locked;
    }

    private LockAttempt noToken(String key, Throwable cause, long started, LockSession session) {
        var error = new LockFailedException("Could not lock key=[" + key + "]", cause);
        log.debug("[{}] Lock failed key=[{}], no fencing token, unlocking it again", settings.applicationName(), key, cause);
        notifyListeners(listener -> listener.onLockFailed(key, error, since(started)));
        return new LockAttempt.NoToken(key, error, session);
    }

    private LockAttempt busy(String key, long started) {
        currentlyProcessingLocks.remove(key);
        log.debug("[{}] Busy key=[{}], held by another instance", settings.applicationName(), key);
        notifyListeners(listener -> listener.onBusy(key, new LockOutcome.Busy<>(false), since(started)));
        return new LockAttempt.Busy(false);
    }

    /**
     * Unlocks {@code key} on the session that locked it. A failed unlock is repeated with a backoff at the end of the
     * queue, until it succeeds or the session ends and takes the lock along. The key stays busy until then.
     */
    private Mono<Void> release(LockSession session, String key) {
        return session.execute(lockConnection -> lockConnection
                        .flatMap(connection -> dao.unlock(connection, key))
                        // the lock went with the ended session
                        .onErrorResume(LockSession.SessionFinishedException.class, _ -> Mono.just(false)))
                .retryWhen(Retry.backoff(Long.MAX_VALUE, settings.unlockRetryMinBackoff())
                        .maxBackoff(settings.unlockRetryMaxBackoff())
                        .doBeforeRetry(signal -> unlockFailed(session, key, signal)))
                // outside the task, safe: usingWhen and releaseAbandonedLock never cancel a release
                .doOnNext(unlocked -> {
                    if (unlocked) {
                        log.debug("[{}] Unlocked key=[{}]", settings.applicationName(), key);
                    } else {
                        log.warn("[{}] Unlock found key=[{}] already released, the lock connection was probably re-created", settings.applicationName(), key);
                    }
                    currentlyProcessingLocks.remove(key);
                })
                .then();
    }

    private void unlockFailed(LockSession session, String key, Retry.RetrySignal signal) {
        if (signal.totalRetries() == 0) {
            log.warn("[{}] Unlock failed key=[{}], repeating it until it succeeds, the key stays busy on this instance", settings.applicationName(), key, signal.failure());
        } else {
            log.debug("[{}] Unlock failed again key=[{}]", settings.applicationName(), key, signal.failure());
        }
        checkConnectionOnError(session, signal.failure());
    }

    private static void checkConnectionOnError(LockSession session, Throwable error) {
        if (rootCause(error) instanceof R2dbcNonTransientResourceException) {
            session.checkAndResetConnection().subscribe();
        }
    }

    private LockSession createSession() {
        return new LockSession(lockConnectionFactory, dao, settings, this::replaceSession);
    }

    private void replaceSession() {
        log.warn("[{}] Lock connection is broken, re-creating it, all its locks are lost", settings.applicationName());
        currentlyProcessingLocks.clear();
        notifyListeners(LockManagerListener::onConnectionReset);
        var nextSession = createSession();
        session = nextSession;
        // close() sets closed before reading the session, so one of the two closes the new one
        if (closed.get()) {
            nextSession.close().subscribe();
        }
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
     * Result of {@link #lock}: locked, locked without a fencing token, or busy.
     */
    private sealed interface LockAttempt {

        /**
         * {@code lockedNanos} from {@link System#nanoTime()} for the hold time, {@code session} holds the lock and alone
         * can release it.
         */
        record Locked(LockHandle handle, long lockedNanos, LockSession session) implements LockAttempt {
        }

        /**
         * Locked, but without a fencing token. The work does not run, {@code tryWithLock} fails with {@code error} once
         * the lock is released.
         */
        record NoToken(String key, LockFailedException error, LockSession session) implements LockAttempt {
        }

        /**
         * Locked by this instance when {@code sameInstance}, otherwise by another one. Nothing to release.
         */
        record Busy(boolean sameInstance) implements LockAttempt {
        }
    }
}
