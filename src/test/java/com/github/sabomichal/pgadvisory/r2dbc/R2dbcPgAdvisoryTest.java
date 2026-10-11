package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.AdvisoryLockListener;
import com.github.sabomichal.pgadvisory.KeyMapper;
import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory;
import com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState;
import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;
import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome;
import com.github.sabomichal.pgadvisory.PgAdvisory.LostLockPolicy;
import com.github.sabomichal.pgadvisory.PgAdvisorySettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import com.github.sabomichal.pgadvisory.spring.FencingR2dbcTransactionManager;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Result;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.reactivestreams.Publisher;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.reactive.TransactionalOperator;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.execute;
import static com.github.sabomichal.pgadvisory.TestPostgres.terminateLockSession;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R2dbcPgAdvisoryTest {

    /** A in these tests does not notice the killed session by itself, so its transactions run into fencing. */
    private static final UnaryOperator<PgAdvisorySettings.Builder> UNAWARE_OF_LOSS =
            settings -> settings.lostLockPolicy(LostLockPolicy.NOTIFY).heartbeatInterval(Duration.ofMinutes(1));

    private ConnectionPool pool;
    private DatabaseClient db;
    private TransactionalOperator transactional;
    private final List<PgAdvisory<?>> instances = new ArrayList<>();

    @BeforeAll
    void createWorkTable() {
        pool = TestPostgres.pool();
        db = DatabaseClient.create(pool);
        transactional = TransactionalOperator.create(new FencingR2dbcTransactionManager(pool));
        await(db.sql("CREATE TABLE IF NOT EXISTS work_item (id serial PRIMARY KEY, lock_key text NOT NULL, writer text NOT NULL)")
                .fetch().rowsUpdated());
    }

    @AfterEach
    void stopInstances() {
        instances.forEach(instance -> await(instance.stop()));
        instances.clear();
    }

    @AfterAll
    void closePool() {
        pool.dispose();
    }

    @Test
    void acquiresIncrementsGenerationAndReleases() {
        var a = advisory("A");
        var key = newKey();

        var first = await(a.withLock(key, handle -> Mono.just(handle.fencingToken())));
        var second = await(a.withLock(key, handle -> Mono.just("done")));

        assertThat(first).isEqualTo(new LockOutcome.Completed<>(1L, 1L));
        assertThat(second).isEqualTo(new LockOutcome.Completed<>("done", 2L));
        assertThat(a.heldLocks()).isZero();
    }

    @Test
    void anotherInstanceGetsBusyWithTheOwner() {
        var a = advisory("A");
        var b = advisory("B");
        var key = newKey();
        var holder = holdLock(a, key);

        assertThat(await(b.withLock(key, handle -> Mono.just("never")))).isEqualTo(new LockOutcome.Busy<>("A"));

        holder.release();
        assertThat(await(b.withLock(key, handle -> Mono.just("now")))).isEqualTo(new LockOutcome.Completed<>("now", 2L));
    }

    @Test
    void sameInstanceGetsHeldLocally() {
        var a = advisory("A");
        var key = newKey();

        var outcome = await(a.withLock(key, handle -> a.withLock(key, inner -> Mono.just("never"))));

        assertThat(outcome).isEqualTo(new LockOutcome.Completed<>(new LockOutcome.HeldLocally<>(), 1L));
    }

    @Test
    void directKeyMapperCollisionOnlyContends() {
        var collidingId = System.nanoTime();
        var colliding = KeyMapper.<String>direct(key -> collidingId, key -> key);
        var a = advisory("A", colliding, UnaryOperator.identity());
        var b = advisory("B", colliding, UnaryOperator.identity());
        var holder = holdLock(a, newKey());

        assertThat(await(b.withLock(newKey(), handle -> Mono.just("never")))).isInstanceOf(LockOutcome.Busy.class);

        holder.release();
    }

    @Test
    void directKeyMapperCollisionIsHeldLocallyWithinOneInstance() {
        var collidingId = System.nanoTime();
        var a = advisory("A", KeyMapper.<String>direct(key -> collidingId, key -> key), UnaryOperator.identity());
        var holder = holdLock(a, newKey());

        // session advisory locks are reentrant: without the local check both keys would run at once
        assertThat(await(a.withLock(newKey(), handle -> Mono.just("never")))).isEqualTo(new LockOutcome.HeldLocally<>());

        holder.release();
    }

    @Test
    void cancelWhileLockingOnTheSessionReleasesTheLock() {
        var a = advisory("A");
        var b = advisory("B");
        var key = newKey();
        // an uncommitted row of the key makes the lock session of A wait inside the acquisition
        var blocker = await(Mono.from(TestPostgres.connectionFactory().create()));
        await(Mono.from(blocker.beginTransaction()).then(execute(blocker, "INSERT INTO advisory_lock (lock_key) VALUES ($1)", key)));

        var cancelled = a.withLock(key, handle -> Mono.just("never")).subscribe();
        sleep(Duration.ofMillis(300));
        cancelled.dispose();
        await(Mono.from(blocker.rollbackTransaction()).then(Mono.from(blocker.close())));

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(await(b.withLock(key, handle -> Mono.just("B")))).isInstanceOf(LockOutcome.Completed.class));
        assertThat(await(a.withLock(key, handle -> Mono.just("A")))).isInstanceOf(LockOutcome.Completed.class);
    }

    @Test
    void cancelWhileTakingOverReleasesTheLock() {
        var a = advisory("A", UNAWARE_OF_LOSS);
        var b = advisory("B", settings -> settings.barrierTimeout(Duration.ofSeconds(10)));
        var c = advisory("C");
        var key = newKey();
        var holder = holdLock(a, key);
        var inFlight = InFlightTransaction.passedFencingCheck(holder.handle());
        terminateLockSession("A");

        var cancelled = b.withLock(key, handle -> Mono.just("never")).subscribe();
        sleep(Duration.ofMillis(500));
        cancelled.dispose();
        inFlight.commit();

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(await(c.withLock(key, handle -> Mono.just("C")))).isInstanceOf(LockOutcome.Completed.class));
        assertThat(await(b.withLock(key, handle -> Mono.just("B")))).isInstanceOf(LockOutcome.Completed.class);
        holder.release();
    }

    @Test
    void failedRecordWriterReleasesTheLock() {
        var a = advisory(R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .workConnectionFactory(pool)
                .recordWriter((connection, table, rowId, key) ->
                        Flux.from(connection.createStatement("SELECT 1 / 0").execute()).flatMap(Result::getRowsUpdated).then())
                .settings(settings("A").build()));
        var b = advisory("B");
        var key = newKey();

        assertThat(await(a.withLock(key, handle -> Mono.just("never")))).isInstanceOf(LockOutcome.Unavailable.class);

        assertThat(await(b.withLock(key, handle -> Mono.just("B")))).isEqualTo(new LockOutcome.Completed<>("B", 1L));
    }

    @Test
    void theSameKeyCanBeLockedRightAfterTheRelease() {
        var a = advisory("A");
        var key = newKey();

        var outcomes = await(a.withLock(key, handle -> Mono.just("first"))
                .flatMap(first -> a.withLock(key, handle -> Mono.just("second")).map(second -> List.of(first, second))));

        assertThat(outcomes).containsExactly(new LockOutcome.Completed<>("first", 1L), new LockOutcome.Completed<>("second", 2L));
    }

    @Test
    void lateTakeoverAfterTheLockWasLostIsRejected() {
        var gate = new WorkConnectionGate(pool);
        var a = advisory(R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .workConnectionFactory(gate)
                .settings(UNAWARE_OF_LOSS.apply(settings("A")).build()));
        var b = advisory("B");
        var key = newKey();

        var late = a.withLock(key, handle -> write(key, "A")).toFuture();
        gate.entered().join();
        // A holds the advisory lock and waits for a work connection when its lock session dies
        terminateLockSession("A");
        var holder = holdLock(b, key);
        gate.open();

        assertThat(late.join()).isInstanceOf(LockOutcome.Unavailable.class);
        assertThat(writers(key)).isEmpty();
        // the token of B is still the current one
        InFlightTransaction.passedFencingCheck(holder.handle()).commit();
        holder.release();
    }

    @Test
    void lateTakeoverAfterANoticedLossIsRejected() {
        var gate = new WorkConnectionGate(pool);
        var restored = new CompletableFuture<Void>();
        var a = advisory(R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .workConnectionFactory(gate)
                .listener(new AdvisoryLockListener() {
                    @Override
                    public void onConnectionRestored() {
                        restored.complete(null);
                    }
                })
                .settings(settings("A").lostLockPolicy(LostLockPolicy.NOTIFY)
                        .heartbeatInterval(Duration.ofMillis(200)).reconnectMinBackoff(Duration.ofMillis(100)).build()));
        var b = advisory("B");
        var key = newKey();

        var late = a.withLock(key, handle -> write(key, "A")).toFuture();
        gate.entered().join();
        terminateLockSession("A");
        restored.orTimeout(10, TimeUnit.SECONDS).join();
        var holder = holdLock(b, key);
        gate.open();

        assertThat(late.join()).isInstanceOf(LockOutcome.Unavailable.class);
        assertThat(writers(key)).isEmpty();
        InFlightTransaction.passedFencingCheck(holder.handle()).commit();
        holder.release();
    }

    @Test
    void lockLostWhileWaitingForTheBarrierIsNotTakenOver() {
        var x = advisory("X", UNAWARE_OF_LOSS);
        var a = advisory("A", UNAWARE_OF_LOSS);
        var b = advisory("B", settings -> settings.barrierTimeout(Duration.ofSeconds(10)));
        var key = newKey();
        var holder = holdLock(x, key);
        var inFlight = InFlightTransaction.passedFencingCheck(holder.handle());
        terminateLockSession("X");

        var waiting = a.withLock(key, handle -> write(key, "A")).toFuture();
        sleep(Duration.ofMillis(500));
        // A holds the advisory lock and waits for the barrier when its lock session dies
        terminateLockSession("A");
        var takeover = b.withLock(key, handle -> write(key, "B")).toFuture();
        sleep(Duration.ofMillis(500));
        inFlight.commit();

        assertThat(waiting.join()).isInstanceOf(LockOutcome.Unavailable.class);
        assertThat(takeover.join()).isEqualTo(new LockOutcome.Completed<>(1L, 2L));
        assertThat(writers(key)).containsExactly("B");
        holder.release();
    }

    @Test
    void transactionInsideTheLockCommits() {
        var a = advisory("A");
        var key = newKey();

        var outcome = await(a.withLock(key, handle -> write(key, "A")));

        assertThat(outcome).isInstanceOf(LockOutcome.Completed.class);
        assertThat(writers(key)).containsExactly("A");
    }

    @Test
    void staleHolderIsFencedAfterTakeover() {
        var a = advisory("A", UNAWARE_OF_LOSS);
        var b = advisory("B");
        var key = newKey();
        var acquired = new CompletableFuture<Void>();
        var proceed = new CompletableFuture<Void>();

        var stale = a.withLock(key, handle -> {
            acquired.complete(null);
            return Mono.fromFuture(proceed).then(write(key, "A"));
        }).toFuture();
        acquired.join();
        assertThat(terminateLockSession("A")).isEqualTo(1);

        var takeover = await(b.withLock(key, handle -> write(key, "B")));
        proceed.complete(null);

        assertThat(takeover).isEqualTo(new LockOutcome.Completed<>(1L, 2L));
        assertThat(stale.join()).isEqualTo(new LockOutcome.Lost<>(1L));
        assertThat(writers(key)).containsExactly("B");
    }

    @Test
    void takeoverWaitsForAnInFlightCommitOfThePreviousHolder() {
        var a = advisory("A", UNAWARE_OF_LOSS);
        var b = advisory("B", settings -> settings.barrierTimeout(Duration.ofSeconds(10)));
        var key = newKey();
        var holder = holdLock(a, key);
        var inFlight = InFlightTransaction.passedFencingCheck(holder.handle());
        terminateLockSession("A");

        var takeover = b.withLock(key, handle -> Mono.just("B")).toFuture();
        sleep(Duration.ofMillis(500));
        assertThat(takeover).isNotDone();

        inFlight.commit();
        assertThat(takeover.join()).isEqualTo(new LockOutcome.Completed<>("B", 2L));
        holder.release();
    }

    @Test
    void barrierTimeoutReturnsBusyAndReleasesTheLock() {
        var a = advisory("A", UNAWARE_OF_LOSS);
        var b = advisory("B", settings -> settings.barrierTimeout(Duration.ofMillis(300)));
        var key = newKey();
        var holder = holdLock(a, key);
        var inFlight = InFlightTransaction.passedFencingCheck(holder.handle());
        terminateLockSession("A");

        assertThat(await(b.withLock(key, handle -> Mono.just("never")))).isEqualTo(new LockOutcome.Busy<>("A"));

        inFlight.commit();
        assertThat(await(b.withLock(key, handle -> Mono.just("B")))).isInstanceOf(LockOutcome.Completed.class);
        holder.release();
    }

    @Test
    void repeatableReadTransactionOfThePreviousHolderDoesNotBlockTheLockSession() {
        var a = advisory("A", UNAWARE_OF_LOSS);
        var b = advisory("B", settings -> settings.barrierTimeout(Duration.ofMillis(300)));
        var key = newKey();
        var holder = holdLock(a, key);
        // holds the shared barrier and, through FOR SHARE, the row lock on the generation until commit
        var inFlight = InFlightTransaction.passedFencingCheck(holder.handle(), IsolationLevel.REPEATABLE_READ);
        terminateLockSession("A");

        var takeover = b.withLock(key, handle -> Mono.just("never")).toFuture();
        var other = b.withLock(newKey(), handle -> Mono.just("other")).block(Duration.ofSeconds(5));

        assertThat(other).isInstanceOf(LockOutcome.Completed.class);
        assertThat(takeover.orTimeout(5, TimeUnit.SECONDS).join()).isEqualTo(new LockOutcome.Busy<>("A"));
        inFlight.commit();
        assertThat(await(b.withLock(key, handle -> Mono.just("B")))).isEqualTo(new LockOutcome.Completed<>("B", 2L));
        holder.release();
    }

    @Test
    void readOnlyTransactionsAreNotFenced() {
        var a = advisory("A");
        for (var isolation : List.of(TransactionDefinition.ISOLATION_READ_COMMITTED, TransactionDefinition.ISOLATION_REPEATABLE_READ,
                TransactionDefinition.ISOLATION_SERIALIZABLE)) {
            var definition = new DefaultTransactionDefinition();
            definition.setIsolationLevel(isolation);
            definition.setReadOnly(true);
            var readOnly = TransactionalOperator.create(new FencingR2dbcTransactionManager(pool), definition);

            var outcome = await(a.withLock(newKey(), handle -> readOnly.transactional(
                    db.sql("SELECT 1").map(row -> row.get(0, Integer.class)).one())));

            assertThat(outcome).isEqualTo(new LockOutcome.Completed<>(1, 1L));
        }
    }

    @Test
    void repeatableReadTransactionWithAStaleSnapshotIsFenced() {
        var a = advisory("A", settings -> settings.lostLockPolicy(LostLockPolicy.NOTIFY));
        var key = newKey();
        var repeatableRead = TransactionalOperator.create(new FencingR2dbcTransactionManager(pool),
                new DefaultTransactionDefinition() {{
                    setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
                }});

        var outcome = await(a.withLock(key, handle -> repeatableRead.transactional(
                insert(key, "A")
                        // a takeover commits after the snapshot of this transaction was taken
                        .then(TestPostgres.withConnection(connection -> execute(connection,
                                "UPDATE advisory_lock SET generation = generation + 1 WHERE id = $1", handle.rowId()))))));

        assertThat(outcome).isEqualTo(new LockOutcome.Lost<>(1L));
        assertThat(writers(key)).isEmpty();
    }

    @Test
    void lostSessionCancelsTheWorkAndReconnects() {
        var lost = new CopyOnWriteArrayList<String>();
        var listener = new AdvisoryLockListener() {
            @Override
            public void onLost(LockHandle handle) {
                lost.add(handle.key());
            }
        };
        var a = advisory(R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .workConnectionFactory(pool)
                .listener(listener)
                .settings(settings("A").heartbeatInterval(Duration.ofMillis(200)).reconnectMinBackoff(Duration.ofMillis(100)).build()));
        var key = newKey();
        var acquired = new CompletableFuture<Void>();

        var running = a.withLock(key, handle -> {
            acquired.complete(null);
            return Mono.never();
        }).toFuture();
        acquired.join();
        terminateLockSession("A");

        assertThat(running.orTimeout(10, TimeUnit.SECONDS).join()).isEqualTo(new LockOutcome.Lost<>(1L));
        assertThat(lost).containsExactly(key);
        await().atMost(Duration.ofSeconds(10)).until(() -> a.state() == ConnectionState.CONNECTED);
        assertThat(await(a.withLock(key, handle -> Mono.just("again")))).isEqualTo(new LockOutcome.Completed<>("again", 2L));
    }

    @Test
    void missingTableFailsStartUnlessAutoCreateIsEnabled() {
        var table = "missing_" + UUID.randomUUID().toString().replace("-", "");
        var strict = R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .settings(PgAdvisorySettings.builder().tableName(table).owner("S").build())
                .build();

        assertThatThrownBy(() -> await(strict.start()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(table);
        // already when the failure arrives, so that a retry can start again
        assertThat(await(strict.start().then(Mono.<ConnectionState>empty()).onErrorResume(_ -> Mono.fromSupplier(strict::state))))
                .isEqualTo(ConnectionState.STOPPED);

        var lenient = advisory("L", settings -> settings.tableName(table));
        assertThat(await(lenient.withLock("k", handle -> Mono.just(1)))).isEqualTo(new LockOutcome.Completed<>(1, 1L));
    }

    @Test
    void stopDrainsRunningWorkAndStartWorksAgain() {
        var a = advisory("A");
        var key = newKey();
        var acquired = new CompletableFuture<Void>();

        var running = a.withLock(key, handle -> {
            acquired.complete(null);
            return Mono.just("finished").delayElement(Duration.ofMillis(300));
        }).toFuture();
        acquired.join();
        await(a.stop());

        assertThat(running.join()).isEqualTo(new LockOutcome.Completed<>("finished", 1L));
        assertThat(await(a.withLock(key, handle -> Mono.just("never")))).isInstanceOf(LockOutcome.Unavailable.class);

        await(a.start());
        assertThat(await(a.withLock(key, handle -> Mono.just("again")))).isEqualTo(new LockOutcome.Completed<>("again", 2L));
    }

    @Test
    void cancelledStopStillDrainsAndClosesTheLockSession() {
        var a = advisory("A");
        var b = advisory("B");
        var key = newKey();
        var acquired = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        a.withLock(key, handle -> {
            acquired.complete(null);
            return Mono.fromFuture(release);
        }).subscribe();
        acquired.join();

        // the caller gives up waiting at once, the stop goes on
        a.stop().subscribe().dispose();
        assertThat(a.state()).isEqualTo(ConnectionState.STOPPING);
        release.complete(null);
        await(a.stop());

        assertThat(a.state()).isEqualTo(ConnectionState.STOPPED);
        assertThat(await(b.withLock(key, handle -> Mono.just("B")))).isInstanceOf(LockOutcome.Completed.class);
    }

    @Test
    void startRightAfterStop() {
        var a = advisory("A");

        await(a.stop().then(a.start()));

        assertThat(a.state()).isEqualTo(ConnectionState.CONNECTED);
        assertThat(await(a.withLock(newKey(), handle -> Mono.just("A")))).isInstanceOf(LockOutcome.Completed.class);
    }

    @Test
    void cancelledStartStillConnects() {
        var a = R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .workConnectionFactory(pool)
                .settings(settings("A").build())
                .build();
        instances.add(a);

        a.start().subscribe().dispose();

        await().atMost(Duration.ofSeconds(10)).until(() -> a.state() == ConnectionState.CONNECTED);
        assertThat(await(a.withLock(newKey(), handle -> Mono.just("A")))).isInstanceOf(LockOutcome.Completed.class);
    }

    @Test
    void stopCancelsWorkAfterTheShutdownTimeout() {
        var a = advisory("A", settings -> settings.shutdownTimeout(Duration.ofMillis(200)));
        var acquired = new CompletableFuture<Void>();

        var running = a.withLock(newKey(), handle -> {
            acquired.complete(null);
            return Mono.never();
        }).toFuture();
        acquired.join();
        await(a.stop());

        assertThat(running.join()).isInstanceOf(LockOutcome.Unavailable.class);
    }

    // ------------------------------------------------------------------ helpers

    private PgAdvisory<String> advisory(String owner) {
        return advisory(owner, UnaryOperator.identity());
    }

    private PgAdvisory<String> advisory(String owner, UnaryOperator<PgAdvisorySettings.Builder> settings) {
        return advisory(owner, KeyMapper.registry(), settings);
    }

    private PgAdvisory<String> advisory(String owner, KeyMapper<String> keyMapper, UnaryOperator<PgAdvisorySettings.Builder> settings) {
        return advisory(R2dbcPgAdvisory.<String>builder(TestPostgres.connectionFactory())
                .workConnectionFactory(pool)
                .keyMapper(keyMapper)
                .settings(settings.apply(settings(owner)).build()));
    }

    private PgAdvisory<String> advisory(R2dbcPgAdvisory.Builder<String> builder) {
        var advisory = builder.build();
        await(advisory.start());
        instances.add(advisory);
        return advisory;
    }

    private static PgAdvisorySettings.Builder settings(String owner) {
        return PgAdvisorySettings.builder().owner(owner).autoCreateSchema(true);
    }

    /** Holds the lock of {@code key} until {@link #release()}. */
    private Holder holdLock(PgAdvisory<String> advisory, String key) {
        var acquired = new CompletableFuture<LockHandle>();
        var release = new CompletableFuture<Void>();
        var outcome = advisory.withLock(key, handle -> {
            acquired.complete(handle);
            return Mono.fromFuture(release);
        }).toFuture();
        return new Holder(acquired.join(), release, outcome);
    }

    private record Holder(LockHandle handle, CompletableFuture<Void> releaseSignal, CompletableFuture<?> outcome) {
        void release() {
            releaseSignal.complete(null);
            outcome.join();
        }
    }

    /** A transaction of the previous holder that passed its fencing check: it holds the barrier (under RR also the row) until commit. */
    private record InFlightTransaction(Connection connection) {

        static InFlightTransaction passedFencingCheck(LockHandle handle) {
            return passedFencingCheck(handle, IsolationLevel.READ_COMMITTED);
        }

        static InFlightTransaction passedFencingCheck(LockHandle handle, IsolationLevel isolation) {
            var connection = await(Mono.from(TestPostgres.connectionFactory().create()));
            var check = await(Mono.from(connection.beginTransaction(isolation)).then(handle.verify(connection)));
            assertThat(check).isInstanceOf(FenceCheck.Valid.class);
            return new InFlightTransaction(connection);
        }

        void commit() {
            await(Mono.from(connection.commitTransaction()).then(Mono.from(connection.close())));
        }
    }

    /** Holds every new work connection until {@link #open()}. */
    private record WorkConnectionGate(ConnectionFactory delegate, CompletableFuture<Void> entered, CompletableFuture<Void> opened)
            implements ConnectionFactory {

        WorkConnectionGate(ConnectionFactory delegate) {
            this(delegate, new CompletableFuture<>(), new CompletableFuture<>());
        }

        @Override
        public Publisher<? extends Connection> create() {
            return Mono.fromRunnable(() -> entered.complete(null))
                    .then(Mono.fromFuture(opened))
                    .then(Mono.from(delegate.create()));
        }

        @Override
        public ConnectionFactoryMetadata getMetadata() {
            return delegate.getMetadata();
        }

        void open() {
            opened.complete(null);
        }
    }

    private Mono<Long> write(String key, String writer) {
        return transactional.transactional(insert(key, writer));
    }

    private Mono<Long> insert(String key, String writer) {
        return db.sql("INSERT INTO work_item (lock_key, writer) VALUES (:key, :writer)")
                .bind("key", key)
                .bind("writer", writer)
                .fetch().rowsUpdated();
    }

    private List<String> writers(String key) {
        return await(db.sql("SELECT writer FROM work_item WHERE lock_key = :key ORDER BY id")
                .bind("key", key)
                .map(row -> row.get(0, String.class))
                .all().collectList());
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
