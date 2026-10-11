package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.AdvisoryLockFencedException;
import com.github.sabomichal.pgadvisory.AdvisoryLockListener;
import com.github.sabomichal.pgadvisory.KeyMapper;
import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.LockRecordWriter;
import com.github.sabomichal.pgadvisory.LockSessionUnavailableException;
import com.github.sabomichal.pgadvisory.PgAdvisory;
import com.github.sabomichal.pgadvisory.PgAdvisorySettings;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Wrapped;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState.CONNECTED;
import static com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState.RECONNECTING;
import static com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState.STARTING;
import static com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState.STOPPED;
import static com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState.STOPPING;

/**
 * {@link PgAdvisory} on R2DBC.
 *
 * <h2>Why fencing</h2>
 * The advisory locks live on one long-lived lock session, while the work commits on other, pooled
 * connections. When the lock session dies, PostgreSQL releases its locks and another instance can take
 * over, but a transaction of the old holder may still be about to commit. Fencing makes sure that such a
 * commit either fails or lands before the new holder starts.
 * <p>
 * The fencing protocol is the {@link Fencer} contract; {@link GenerationFencer} implements it with the
 * generation of the key's row as the fencing token and a two-int advisory lock as the barrier between the old
 * and the new holder.
 *
 * <h2>Acquisition</h2>
 * <ol>
 *   <li>{@code lockOnSession}: on the lock session in autocommit, find or create the key's row and try the
 *       advisory lock; {@code false} means {@code Busy}. The lock session never waits for other holders: all its
 *       work is serialized, so a wait there would stall every key and the heartbeat.</li>
 *   <li>{@code takeOver}: in a transaction on a work connection, {@link Fencer#takeOver} waits for the previous
 *       holder's transactions that passed their check and issues the new fencing token, but commits it only if the
 *       lock session of step 1 still holds the lock: in between it may have been lost and taken over by another
 *       holder. The {@link LockRecordWriter} runs in the same transaction. Only then the work runs.</li>
 * </ol>
 * The advisory lock is the resource of a {@code usingWhen}: whatever ends the acquisition or the work, a failure,
 * a timeout or the caller cancelling, releases it, and only then the key's local registration.
 *
 * <h2>Work transactions</h2>
 * Right before commit, {@code FencingR2dbcTransactionManager} (or {@link #fence}) calls
 * {@link LockHandle#verify}, that is {@link Fencer#verify}. A stale token rolls the transaction back with
 * {@link AdvisoryLockFencedException} and the outcome is {@code Lost}.
 */
public final class R2dbcPgAdvisory<K> implements PgAdvisory<K> {

    private static final Logger log = LoggerFactory.getLogger(R2dbcPgAdvisory.class);
    private static final Duration IDLE_POLL_INTERVAL = Duration.ofMillis(20);
    private static final Duration CANCEL_GRACE_PERIOD = Duration.ofSeconds(5);

    private final ConnectionFactory workConnectionFactory;
    private final KeyMapper<K> keyMapper;
    private final PgAdvisorySettings settings;
    private final List<AdvisoryLockListener> listeners;
    private final @Nullable LockRecordWriter recordWriter;
    private final LockStore store;
    private final LockSession session;
    private final Fencer fencer;

    private final Map<Object, Registration> active = new ConcurrentHashMap<>();
    private final AtomicReference<ConnectionState> state = new AtomicReference<>(STOPPED);
    private volatile Disposable heartbeat = Disposables.disposed();
    private volatile Disposable reconnect = Disposables.disposed();
    /** Completes when the last {@link #stop()} has finished. */
    private volatile Mono<Void> stopping = Mono.empty();

    private R2dbcPgAdvisory(Builder<K> builder) {
        this.workConnectionFactory = builder.workConnectionFactory;
        this.keyMapper = builder.keyMapper;
        this.settings = builder.settings;
        this.listeners = List.copyOf(builder.listeners);
        this.recordWriter = builder.recordWriter;
        this.store = new R2dbcLockStore(settings.tableName());
        this.session = new R2dbcLockSession(builder.lockConnectionFactory, store, "pg-advisory:" + settings.owner());
        this.fencer = new GenerationFencer(store, settings.fenceNamespace());
    }

    /** @param lockConnectionFactory unpooled factory for the long-lived lock session */
    public static <K> Builder<K> builder(ConnectionFactory lockConnectionFactory) {
        return new Builder<>(lockConnectionFactory);
    }

    // ------------------------------------------------------------------ lifecycle

    @Override
    public Mono<Void> start() {
        return Mono.defer(() -> {
            if (!state.compareAndSet(STOPPED, STARTING)) {
                return Mono.error(new IllegalStateException("PgAdvisory is " + state.get()));
            }
            return detached(session.open()
                    .then(session.execute(connection -> store.ensureTable(connection, settings.autoCreateSchema())))
                    .doOnSuccess(_ -> started())
                    // doOnTerminate, not doFinally: the caller already sees STOPPED when the error reaches it
                    .onErrorResume(error -> session.close()
                            .doOnTerminate(() -> state.compareAndSet(STARTING, STOPPED))
                            .then(Mono.error(error))));
        });
    }

    @Override
    public Mono<Void> stop() {
        return Mono.defer(() -> {
            ConnectionState current;
            do {
                current = state.get();
                if (current == STOPPED) {
                    return Mono.empty();
                }
                if (current == STOPPING) {
                    return stopping;
                }
                // retried: a lost lock session may move CONNECTED to RECONNECTING meanwhile
            } while (!state.compareAndSet(current, STOPPING));
            heartbeat.dispose();
            reconnect.dispose();
            stopping = detached(awaitIdle(settings.shutdownTimeout())
                    .onErrorResume(TimeoutException.class, _ -> cancelRunningWork())
                    .then(session.close())
                    // doOnTerminate, not doFinally: the caller already sees STOPPED when the stop completes
                    .doOnTerminate(this::stopped));
            return stopping;
        });
    }

    /**
     * Runs {@code operation} to its end even if the caller cancels: the caller only stops waiting for it.
     * A start or stop cut in the middle would leave the state and the lock session out of step.
     */
    private static Mono<Void> detached(Mono<Void> operation) {
        var result = operation.cache();
        result.subscribe(null, _ -> {
        });
        return result;
    }

    private void started() {
        if (!state.compareAndSet(STARTING, CONNECTED)) {
            // stopped while starting
            return;
        }
        heartbeat = startHeartbeat();
        log.info("PgAdvisory started: owner [{}], table [{}]", settings.owner(), settings.tableName());
    }

    private void stopped() {
        state.set(STOPPED);
        log.info("PgAdvisory stopped: owner [{}]", settings.owner());
    }

    private Mono<Void> awaitIdle(Duration timeout) {
        return Flux.interval(Duration.ZERO, IDLE_POLL_INTERVAL)
                .filter(_ -> active.isEmpty())
                .next()
                .then()
                .timeout(timeout);
    }

    private Mono<Void> cancelRunningWork() {
        log.warn("PgAdvisory stop: cancelling [{}] running works after the shutdown timeout", active.size());
        active.values().forEach(Registration::cancel);
        return awaitIdle(CANCEL_GRACE_PERIOD).onErrorComplete();
    }

    @Override
    public ConnectionState state() {
        return state.get();
    }

    @Override
    public int heldLocks() {
        return (int) active.values().stream().filter(Registration::isHeld).count();
    }

    @Override
    public String owner() {
        return settings.owner();
    }

    // ------------------------------------------------------------------ locking

    @Override
    public <T> Mono<LockOutcome<T>> withLock(K key, Function<LockHandle, Mono<T>> work) {
        return withLock(key, settings.lostLockPolicy(), work);
    }

    @Override
    public <T> Mono<LockOutcome<T>> withLock(K key, LostLockPolicy lostLockPolicy, Function<LockHandle, Mono<T>> work) {
        return Mono.defer(() -> {
            var started = System.nanoTime();
            var lockKey = keyMapper.keyText(key);
            if (state.get() != CONNECTED) {
                return failed(lockKey, new LockOutcome.Unavailable<>(new LockSessionUnavailableException()), started);
            }
            var localKey = keyMapper.localKey(key);
            var registration = new Registration();
            // Required for mutual exclusion, not just to save a round trip: all locks share one session and
            // session advisory locks are reentrant, so pg_try_advisory_lock would grant the key a second time.
            // The registration lives until the advisory lock is released, see release.
            if (active.putIfAbsent(localKey, registration) != null) {
                return failed(lockKey, new LockOutcome.HeldLocally<>(), started);
            }
            // Whatever lockOnSession acquires is released exactly once: by usingWhen once it was delivered,
            // otherwise (the caller cancelled while the lock session was still working) by the abandon callback.
            return Mono.usingWhen(lockOnSession(key, lockKey, localKey, registration),
                    acquisition -> switch (acquisition) {
                        case SessionAcquisition.Locked locked -> takeOver(locked.lock())
                                .flatMap(takeOver -> switch (takeOver) {
                                    case TakeOver.Acquired acquired -> holdWhileWorking(acquired.handle(), registration, lostLockPolicy, work, started);
                                    case TakeOver.Busy busy -> failed(lockKey, new LockOutcome.Busy<>(busy.owner()), started);
                                    case TakeOver.Unavailable unavailable -> failed(lockKey, new LockOutcome.Unavailable<>(unavailable.cause()), started);
                                });
                        case SessionAcquisition.Busy busy -> failed(lockKey, new LockOutcome.Busy<>(busy.owner()), started);
                        case SessionAcquisition.Unavailable unavailable -> failed(lockKey, new LockOutcome.Unavailable<>(unavailable.cause()), started);
                    },
                    acquisition -> release(acquisition, localKey, registration),
                    (acquisition, _) -> release(acquisition, localKey, registration),
                    acquisition -> release(acquisition, localKey, registration));
        });
    }

    @Override
    public Mono<FenceCheck> fence(Connection connection) {
        return LockHandle.current()
                .flatMap(handle -> handle.verify(connection))
                .defaultIfEmpty(new FenceCheck.NoLock());
    }

    /**
     * Step 1 of the acquisition, on the lock session in autocommit: find or create the key's row and try the
     * advisory lock. Nothing here waits for other holders, so the serialized lock session stays responsive.
     */
    private Mono<SessionAcquisition> lockOnSession(K key, String lockKey, Object localKey, Registration registration) {
        return session.execute(connection -> tryLock(connection, key, lockKey),
                        abandoned -> release(abandoned, localKey, registration).subscribe())
                .onErrorResume(error -> handleSessionError(error).thenReturn(new SessionAcquisition.Unavailable(error)));
    }

    private Mono<SessionAcquisition> tryLock(Connection connection, K key, String lockKey) {
        // runs on the lock session, so this is the incarnation of connection
        var incarnation = session.incarnation().orElseThrow(LockSessionUnavailableException::new);
        return store.findOrCreateRow(connection, lockKey).flatMap(rowId -> {
            var lockId = keyMapper.lockId(key, rowId);
            return store.tryLock(connection, lockId).flatMap(locked -> locked
                    ? Mono.just(new SessionAcquisition.Locked(new SessionLock(lockKey, rowId, lockId, incarnation)))
                    : store.owner(connection, rowId).map(owner -> new SessionAcquisition.Busy(owner.orElse(null))));
        });
    }

    /**
     * Step 2 of the acquisition, in a transaction on a work connection: {@link Fencer#takeOver} waits for the
     * previous holder and issues the fencing token, then the {@link LockRecordWriter} runs. A timeout
     * ({@code barrierTimeout}) means the previous holder is still committing: the outcome is {@code Busy}. A lock
     * lost since step 1 means another holder may have taken over: the outcome is {@code Unavailable}. On any
     * failure the transaction rolls back and the caller releases the lock.
     */
    private Mono<TakeOver> takeOver(SessionLock lock) {
        return Mono.usingWhen(workConnectionFactory.create(),
                        connection -> store.inTransaction(connection, fencer.takeOver(connection, lock, settings.owner(), settings.barrierTimeout(),
                                                lockStillHeld(lock))
                                        .flatMap(token -> writeRecord(connection, lock).thenReturn(token)))
                                .<TakeOver>map(token -> new TakeOver.Acquired(new DefaultLockHandle(lock.key(), lock.rowId(), lock.lockId(),
                                        token, settings.owner(), lock.incarnation(), fencer)))
                                .onErrorResume(error -> SqlStates.is(error, SqlStates.LOCK_NOT_AVAILABLE)
                                        ? store.owner(connection, lock.rowId()).map(owner -> new TakeOver.Busy(owner.orElse(null)))
                                        : Mono.just(new TakeOver.Unavailable(error))),
                        Connection::close)
                .onErrorResume(error -> Mono.just(new TakeOver.Unavailable(error)));
    }

    /**
     * A session advisory lock ends only with an unlock on its session, which nobody issues during the takeover,
     * or with the session's backend. So a round trip on the same incarnation of the lock session proves the lock.
     */
    private Mono<Boolean> lockStillHeld(SessionLock lock) {
        return session.executeIfCurrent(lock.incarnation(), connection -> store.ping(connection).thenReturn(true))
                .timeout(settings.heartbeatTimeout())
                .defaultIfEmpty(false)
                .onErrorReturn(false);
    }

    private Mono<Void> writeRecord(Connection connection, SessionLock lock) {
        return recordWriter == null ? Mono.empty() : recordWriter.onAcquire(connection, store.tableName(), lock.rowId(), lock.key());
    }

    private <T> Mono<LockOutcome<T>> holdWhileWorking(DefaultLockHandle handle, Registration registration, LostLockPolicy lostLockPolicy,
                                                      Function<LockHandle, Mono<T>> work, long started) {
        registration.handle = handle;
        if (session.incarnation().orElse(-1) != handle.incarnation()) {
            // the session dropped between the acquisition and the registration
            markLost(handle);
        }
        notifyListeners(listener -> listener.onAcquired(handle, since(started)));
        return runWork(handle, registration, lostLockPolicy, work);
    }

    private <T> Mono<LockOutcome<T>> runWork(DefaultLockHandle handle, Registration registration, LostLockPolicy lostLockPolicy,
                                             Function<LockHandle, Mono<T>> work) {
        var running = Mono.defer(() -> work.apply(handle))
                .contextWrite(context -> context.put(LockHandle.CONTEXT_KEY, handle))
                .takeUntilOther(registration.cancelSignal());
        if (lostLockPolicy == LostLockPolicy.CANCEL_WORK) {
            running = running.takeUntilOther(handle.lostSignal());
        }
        return running
                .map(value -> completedOrLost(handle, value))
                .switchIfEmpty(Mono.fromSupplier(() -> emptyOutcome(handle, registration)))
                .onErrorResume(R2dbcPgAdvisory::isFenced, _ -> {
                    markLost(handle);
                    return Mono.just(new LockOutcome.Lost<>(handle.fencingToken()));
                });
    }

    /** A commit of the work was fenced off, possibly wrapped by the transaction infrastructure. */
    private static boolean isFenced(Throwable error) {
        for (var e = error; e != null; e = e.getCause()) {
            if (e instanceof AdvisoryLockFencedException) {
                return true;
            }
        }
        return false;
    }

    private <T> LockOutcome<T> completedOrLost(DefaultLockHandle handle, @Nullable T value) {
        return handle.isLost()
                ? new LockOutcome.Lost<>(handle.fencingToken())
                : new LockOutcome.Completed<>(value, handle.fencingToken());
    }

    /** The work completed without a value, or it was cut off by a lost lock or by stop. */
    private <T> LockOutcome<T> emptyOutcome(DefaultLockHandle handle, Registration registration) {
        if (registration.isCancelled()) {
            return new LockOutcome.Unavailable<>(new LockSessionUnavailableException());
        }
        return completedOrLost(handle, null);
    }

    /**
     * Releases what {@link #lockOnSession} acquired ({@code null} when nothing) and only then drops the local
     * registration, so that the key cannot be locked again while the advisory lock is still held.
     */
    private Mono<Void> release(@Nullable SessionAcquisition acquisition, Object localKey, Registration registration) {
        var handle = registration.handle;
        if (handle != null) {
            handle.markReleased();
        }
        var unlock = acquisition instanceof SessionAcquisition.Locked(var lock) ? unlock(lock) : Mono.<Void>empty();
        // doOnTerminate, not doFinally: usingWhen emits the outcome only after this completes, and a caller
        // continuing with the same key must already find it free
        return unlock.doOnTerminate(() -> {
            active.remove(localKey, registration);
            if (handle != null) {
                notifyListeners(listener -> listener.onReleased(handle, since(handle.acquiredNanos())));
            }
        });
    }

    private Mono<Void> unlock(SessionLock lock) {
        return session.executeIfCurrent(lock.incarnation(), connection -> store.unlock(connection, lock.lockId()))
                .doOnNext(unlocked -> {
                    if (!unlocked) {
                        log.warn("Advisory lock [{}] was not held by the lock session on unlock", lock.key());
                    }
                })
                .then()
                .onErrorResume(error -> {
                    // the lock might still be held; a broken session is discarded together with its locks
                    log.warn("Failed to unlock advisory lock [{}]", lock.key(), error);
                    return handleSessionError(error);
                });
    }

    private <T> Mono<LockOutcome<T>> failed(String key, LockOutcome<T> outcome, long started) {
        notifyListeners(listener -> listener.onAcquireFailed(key, outcome, since(started)));
        return Mono.just(outcome);
    }

    // ------------------------------------------------------------------ lock session health

    private Disposable startHeartbeat() {
        return Flux.interval(settings.heartbeatInterval())
                .filter(_ -> state.get() == CONNECTED)
                .concatMap(_ -> session.isAlive().timeout(settings.heartbeatTimeout()).onErrorReturn(false))
                .filter(alive -> !alive)
                .subscribe(_ -> connectionLost(new IllegalStateException("Lock session heartbeat failed")));
    }

    private Mono<Void> handleSessionError(Throwable error) {
        return session.isBroken(error)
                .doOnNext(broken -> {
                    if (broken) {
                        connectionLost(error);
                    }
                })
                .then();
    }

    private void connectionLost(Throwable cause) {
        if (!state.compareAndSet(CONNECTED, RECONNECTING)) {
            return;
        }
        log.warn("Advisory lock session lost, all locks of [{}] are released", settings.owner(), cause);
        session.discard();
        active.values().forEach(registration -> {
            var handle = registration.handle;
            if (handle != null) {
                markLost(handle);
            }
        });
        notifyListeners(listener -> listener.onConnectionLost(cause));
        reconnect = startReconnect();
    }

    private Disposable startReconnect() {
        var backoff = Retry.backoff(Long.MAX_VALUE, settings.reconnectMinBackoff())
                .maxBackoff(settings.reconnectMaxBackoff())
                .doBeforeRetry(signal -> log.warn("Advisory lock session reconnect failed, retrying", signal.failure()));
        return session.open()
                .retryWhen(backoff)
                .subscribe(null, error -> log.error("Advisory lock session reconnect gave up", error), this::reconnected);
    }

    private void reconnected() {
        if (state.compareAndSet(RECONNECTING, CONNECTED)) {
            log.info("Advisory lock session reconnected");
            notifyListeners(AdvisoryLockListener::onConnectionRestored);
        } else {
            session.close().subscribe();
        }
    }

    private void markLost(DefaultLockHandle handle) {
        if (handle.markLost()) {
            notifyListeners(listener -> listener.onLost(handle));
        }
    }

    private void notifyListeners(Consumer<AdvisoryLockListener> callback) {
        for (var listener : listeners) {
            try {
                callback.accept(listener);
            } catch (RuntimeException e) {
                log.warn("Advisory lock listener [{}] failed", listener, e);
            }
        }
    }

    private static Duration since(long startedNanos) {
        return Duration.ofNanos(System.nanoTime() - startedNanos);
    }

    // ------------------------------------------------------------------ types

    /** Result of step 1 of the acquisition, on the lock session. */
    private sealed interface SessionAcquisition {

        record Locked(SessionLock lock) implements SessionAcquisition {
        }

        record Busy(@Nullable String owner) implements SessionAcquisition {
        }

        record Unavailable(Throwable cause) implements SessionAcquisition {
        }
    }

    /** Result of step 2 of the acquisition, on a work connection. */
    private sealed interface TakeOver {

        record Acquired(DefaultLockHandle handle) implements TakeOver {
        }

        /** The previous holder, {@code owner}, is still committing. */
        record Busy(@Nullable String owner) implements TakeOver {
        }

        record Unavailable(Throwable cause) implements TakeOver {
        }
    }

    /** A key being locked or held by this instance; prevents locking the same key twice locally. */
    private static final class Registration {

        private volatile @Nullable DefaultLockHandle handle;
        private volatile boolean cancelled;
        private final Sinks.One<Boolean> cancelSignal = Sinks.one();

        void cancel() {
            cancelled = true;
            cancelSignal.tryEmitValue(true);
        }

        boolean isCancelled() {
            return cancelled;
        }

        Mono<Boolean> cancelSignal() {
            return cancelSignal.asMono();
        }

        boolean isHeld() {
            var current = handle;
            return current != null && current.isHeld();
        }
    }

    public static final class Builder<K> {

        private final ConnectionFactory lockConnectionFactory;
        private ConnectionFactory workConnectionFactory;
        private KeyMapper<K> keyMapper = KeyMapper.registry();
        private PgAdvisorySettings settings = PgAdvisorySettings.defaults();
        private final List<AdvisoryLockListener> listeners = new ArrayList<>();
        private @Nullable LockRecordWriter recordWriter;

        private Builder(ConnectionFactory lockConnectionFactory) {
            this.lockConnectionFactory = lockConnectionFactory;
            this.workConnectionFactory = lockConnectionFactory;
        }

        /** Usually the application's pool; lends one connection to each takeover transaction. Defaults to the lock connection factory. */
        public Builder<K> workConnectionFactory(ConnectionFactory workConnectionFactory) {
            this.workConnectionFactory = workConnectionFactory;
            return this;
        }

        public Builder<K> keyMapper(KeyMapper<K> keyMapper) {
            this.keyMapper = keyMapper;
            return this;
        }

        public Builder<K> settings(PgAdvisorySettings settings) {
            this.settings = settings;
            return this;
        }

        public Builder<K> listener(AdvisoryLockListener listener) {
            this.listeners.add(listener);
            return this;
        }

        public Builder<K> listeners(List<? extends AdvisoryLockListener> listeners) {
            this.listeners.addAll(listeners);
            return this;
        }

        public Builder<K> recordWriter(@Nullable LockRecordWriter recordWriter) {
            this.recordWriter = recordWriter;
            return this;
        }

        public R2dbcPgAdvisory<K> build() {
            if (lockConnectionFactory instanceof Wrapped<?>) {
                log.warn("The lock connection factory [{}] looks like a pool; pool eviction or max life time would close "
                        + "the lock session and release all locks. Use an unpooled ConnectionFactory.", lockConnectionFactory.getClass().getName());
            }
            return new R2dbcPgAdvisory<>(this);
        }
    }
}
