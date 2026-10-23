package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientResourceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.execute;
import static com.github.sabomichal.pgadvisory.TestPostgres.terminateConnections;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R2dbcLockManagerTest {

    private static final Logger log = LoggerFactory.getLogger(R2dbcLockManagerTest.class);

    private final List<R2dbcLockManager> lockManagers = new ArrayList<>();

    @AfterEach
    void closeLockManagers() {
        lockManagers.forEach(R2dbcLockManager::close);
        lockManagers.clear();
    }

    @Test
    void locksMultipleKeys() {
        var lockManager = lockManager(settings("A"));
        var keys = Flux.range(0, 10).map(_ -> newKey()).collectList().block();

        var outcomes = await(Flux.fromIterable(keys)
                .flatMap(key -> lockManager.tryWithLock(key, handle -> Mono.just(handle.key()).delayElement(Duration.ofMillis(10))))
                .collectList());

        assertThat(outcomes).containsExactlyInAnyOrderElementsOf(completed(keys));
    }

    @Test
    void lockIsNotReentrant() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();
        var holder = hold(lockManager, key);

        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Busy<>(true));
        var other = newKey();
        assertThat(lockAndUnlock(lockManager, other)).isEqualTo(new LockOutcome.Completed<>(other));

        holder.release();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void anotherInstanceCannotLockTheKey() {
        var a = lockManager(settings("A"));
        var b = lockManager(settings("B"));
        var key = newKey();
        var holder = hold(a, key);

        assertThat(lockAndUnlock(b, key)).isEqualTo(new LockOutcome.Busy<>(false));

        holder.release();
        assertThat(lockAndUnlock(b, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void workGetsItsHandleAlsoInTheReactorContext() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();

        var outcome = await(lockManager.tryWithLock(key, handle -> LockHandle.current()
                .doOnNext(current -> assertThat(current).isSameAs(handle))
                .map(LockHandle::key)));

        assertThat(outcome).isEqualTo(new LockOutcome.Completed<>(key));
        assertThat(await(LockHandle.current())).isNull();
    }

    @Test
    void callsOnLockBeforeAndOnFinallyAfterTheWork() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();
        var calls = new CopyOnWriteArrayList<String>();

        await(lockManager.tryWithLock(key, handle -> calls.add("lock " + handle.key()), handle -> calls.add("finally " + handle.key()),
                _ -> Mono.fromRunnable(() -> calls.add("work"))));

        assertThat(calls).containsExactly("lock " + key, "work", "finally " + key);
    }

    @Test
    void busyKeyDoesNotCallTheCallbacks() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();
        var holder = hold(lockManager, key);
        var calls = new CopyOnWriteArrayList<String>();

        var outcome = await(lockManager.tryWithLock(key, _ -> calls.add("lock"), _ -> calls.add("finally"), _ -> Mono.just("work")));

        assertThat(outcome).isEqualTo(new LockOutcome.Busy<>(true));
        assertThat(calls).isEmpty();
        holder.release();
    }

    @Test
    void errorOfTheWorkIsPropagatedAndTheKeyIsUnlocked() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();
        var failure = new IllegalStateException("work failed");

        assertThatThrownBy(() -> await(lockManager.tryWithLock(key, _ -> Mono.error(failure)))).isSameAs(failure);

        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void theSameKeyCanBeLockedRightAfterTheRelease() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();

        var outcomes = await(lockManager.tryWithLock(key, _ -> Mono.just("first"))
                .flatMap(first -> lockManager.tryWithLock(key, _ -> Mono.just("second")).map(second -> List.of(first, second))));

        assertThat(outcomes).containsExactly(new LockOutcome.Completed<>("first"), new LockOutcome.Completed<>("second"));
    }

    @Test
    void emptyWorkCompletesEmptyAndUnlocks() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();

        assertThat(await(lockManager.tryWithLock(key, _ -> Mono.empty()))).isNull();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void unreachableDatabaseFailsWithLockFailedException() {
        var lockManager = lockManager(ConnectionFactories.get("r2dbc:postgresql://user:password@localhost:1/db"), settings("A"));
        var workRan = new AtomicBoolean();

        assertThatThrownBy(() -> await(lockManager.tryWithLock(newKey(), _ -> Mono.fromRunnable(() -> workRan.set(true)))))
                .isInstanceOf(LockFailedException.class)
                .hasCauseInstanceOf(R2dbcNonTransientResourceException.class);
        assertThat(workRan).isFalse();
    }

    @Test
    void failingCommitDoesNotLeakTheLock() {
        var a = lockManager(failingCommits(TestPostgres.connectionFactory()), settings("A"));
        var b = lockManager(settings("B"));
        var key = newKey();
        // the key's row exists, otherwise a rollback would remove it and the next lock would get a new id
        lockAndUnlock(b, key);

        // a session advisory lock survives the rollback of the transaction it was taken in
        await(a.tryWithLock(key, handle -> Mono.just(handle.key())).onErrorResume(LockFailedException.class, _ -> Mono.empty()));

        assertThat(lockAndUnlock(b, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void cancelWhileLockingReleasesTheLock() {
        var listener = new RecordingListener();
        var a = lockManager(settings("A"), listener);
        var b = lockManager(settings("B"));
        var key = newKey();
        // an uncommitted row of the key makes the INSERT of A wait until its caller gives up
        var blocker = await(Mono.<Connection>from(TestPostgres.connectionFactory().create()));
        await(Mono.from(blocker.beginTransaction()).then(execute(blocker, "INSERT INTO advisory_lock (lock_key) VALUES ($1)", key)));

        assertThat(await(a.tryWithLock(key, _ -> Mono.just("never")).timeout(Duration.ofMillis(300), Mono.empty()))).isNull();
        await(Mono.from(blocker.rollbackTransaction()).then(Mono.from(blocker.close())));

        // onReleased comes after the key was removed from memory
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(listener.calls).containsExactly("locked " + key, "released " + key));
        assertThat(lockAndUnlock(b, key)).isEqualTo(new LockOutcome.Completed<>(key));
        assertThat(lockAndUnlock(a, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void reconnectsWhenTheLockConnectionIsBroken() {
        var listener = new RecordingListener();
        var applicationName = "lock-manager-" + UUID.randomUUID();
        var lockManager = lockManager(settings(applicationName), listener);
        lockAndUnlock(lockManager, newKey());

        assertThat(terminateConnections(applicationName)).isEqualTo(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            await(lockManager.validateDbConnection());
            assertThat(listener.calls).containsOnlyOnce("reset");
        });

        var key = newKey();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void listenerSeesLockedBusyAndReleased() {
        var listenerA = new RecordingListener();
        var listenerB = new RecordingListener();
        var a = lockManager(settings("A"), listenerA);
        var b = lockManager(settings("B"), listenerB);
        var key = newKey();
        var holder = hold(a, key);

        lockAndUnlock(a, key);
        lockAndUnlock(b, key);
        holder.release();

        assertThat(listenerA.calls).containsExactly("locked " + key, "busy " + key + " here", "released " + key);
        assertThat(listenerB.calls).containsExactly("busy " + key + " elsewhere");
    }

    @Test
    void listenerSeesTheReleaseAfterAFailedWork() {
        var listener = new RecordingListener();
        var lockManager = lockManager(settings("A"), listener);
        var key = newKey();

        assertThatThrownBy(() -> await(lockManager.tryWithLock(key, _ -> Mono.error(new IllegalStateException("work failed")))))
                .hasMessage("work failed");

        assertThat(listener.calls).containsExactly("locked " + key, "released " + key);
    }

    @Test
    void listenerSeesTheFailedLock() {
        var listener = new RecordingListener();
        var lockManager = lockManager(ConnectionFactories.get("r2dbc:postgresql://user:password@localhost:1/db"), settings("A"), listener);
        var key = newKey();

        assertThatThrownBy(() -> await(lockManager.tryWithLock(key, _ -> Mono.just("work")))).isInstanceOf(LockFailedException.class);

        assertThat(listener.calls).containsExactly("failed " + key);
    }

    @Test
    void failingListenerDoesNotBreakLocking() {
        var failing = new LockManagerListener() {
            @Override
            public void onLocked(LockHandle handle, Duration lockDuration) {
                throw new IllegalStateException("listener failed");
            }

            @Override
            public void onReleased(LockHandle handle, Duration heldDuration) {
                throw new IllegalStateException("listener failed");
            }
        };
        var recording = new RecordingListener();
        var lockManager = lockManager(settings("A"), failing, recording);
        var key = newKey();

        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
        assertThat(recording.calls).containsExactly("locked " + key, "released " + key);
    }

    @Test
    void createsAndClosesOnAnInterruptedThread() {
        var other = lockManager(settings("B"));
        var key = newKey();

        Thread.currentThread().interrupt();
        var lockManager = lockManager(settings("A"));
        assertThat(Thread.interrupted()).isTrue();
        hold(lockManager, key);

        Thread.currentThread().interrupt();
        lockManager.close();
        assertThat(Thread.interrupted()).isTrue();

        // PostgreSQL releases the session advisory lock once the closed connection's backend exits
        await().atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(lockAndUnlock(other, key)).isEqualTo(new LockOutcome.Completed<>(key)));
    }

    @Test
    void missingTableFailsUnlessAutoCreateIsEnabled() {
        var table = "missing_" + UUID.randomUUID().toString().replace("-", "");

        assertThatThrownBy(() -> await(lockManager(settings("A").tableName(table)).ensureTable()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(table);

        var lockManager = lockManager(settings("B").tableName(table).autoCreateSchema(true));
        await(lockManager.ensureTable());
        var key = newKey();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void locksManyKeysInTime() {
        var lockManager = lockManager(settings("A"));
        var keys = Flux.range(0, 1000).map(_ -> newKey()).collectList().block();

        var started = System.nanoTime();
        var outcomes = Flux.fromIterable(keys)
                .flatMap(key -> lockManager.tryWithLock(key, handle -> Mono.just(handle.key())))
                .collectList()
                .block(Duration.ofSeconds(120));
        var elapsed = Duration.ofNanos(System.nanoTime() - started);

        log.info("{} lock/unlock cycles in {} ms ({}/s)", keys.size(), elapsed.toMillis(),
                keys.size() * 1000L / Math.max(1, elapsed.toMillis()));
        assertThat(outcomes).containsExactlyInAnyOrderElementsOf(completed(keys));
        // the polled mutex of the lock connection took 32 s, now it takes 3.8 s on Docker Desktop for Windows
        assertThat(elapsed).isLessThan(Duration.ofSeconds(15));
    }

    @Test
    void contendedKeysAreNeverHeldTwice() {
        var a = lockManager(settings("A"));
        var b = lockManager(settings("B"));
        var keys = Flux.range(0, 10).map(_ -> newKey()).collectList().block();
        var holders = new ConcurrentHashMap<String, AtomicInteger>();
        var overlaps = new AtomicInteger();

        // A and B take turns on the same keys
        var outcomes = Flux.range(0, 1000)
                .flatMap(i -> (i % 2 == 0 ? a : b).tryWithLock(keys.get((i / 2) % keys.size()), handle -> {
                    var holder = holders.computeIfAbsent(handle.key(), _ -> new AtomicInteger());
                    if (holder.incrementAndGet() > 1) {
                        overlaps.incrementAndGet();
                    }
                    return Mono.delay(Duration.ofMillis(1)).thenReturn(handle.key()).doFinally(_ -> holder.decrementAndGet());
                }), 512)
                .collectList()
                .block(Duration.ofSeconds(120));

        assertThat(overlaps).hasValue(0);
        assertThat(outcomes).hasSize(1000)
                .hasAtLeastOneElementOfType(LockOutcome.Completed.class)
                .hasAtLeastOneElementOfType(LockOutcome.Busy.class);
    }

    // ------------------------------------------------------------------ helpers

    private R2dbcLockManager lockManager(LockManagerSettings.Builder settings, LockManagerListener... listeners) {
        return lockManager(TestPostgres.connectionFactory(), settings, listeners);
    }

    private R2dbcLockManager lockManager(ConnectionFactory connectionFactory, LockManagerSettings.Builder settings,
                                         LockManagerListener... listeners) {
        var lockManager = new R2dbcLockManager(connectionFactory, settings.build(), List.of(listeners));
        lockManagers.add(lockManager);
        return lockManager;
    }

    private static LockManagerSettings.Builder settings(String applicationName) {
        return LockManagerSettings.builder().applicationName(applicationName);
    }

    /** Holds the lock of {@code key} until {@link Holder#release()}. */
    private static Holder hold(R2dbcLockManager lockManager, String key) {
        var locked = new CompletableFuture<Void>();
        var release = new CompletableFuture<Void>();
        var outcome = lockManager.tryWithLock(key, handle -> {
            locked.complete(null);
            return Mono.fromFuture(release).thenReturn(handle.key());
        }).toFuture();
        locked.join();
        return new Holder(release, outcome);
    }

    private record Holder(CompletableFuture<Void> releaseSignal, CompletableFuture<LockOutcome<String>> outcome) {
        void release() {
            releaseSignal.complete(null);
            outcome.join();
        }
    }

    /** Records the callbacks as text. */
    private static class RecordingListener implements LockManagerListener {

        final List<String> calls = new CopyOnWriteArrayList<>();

        @Override
        public void onLocked(LockHandle handle, Duration lockDuration) {
            calls.add("locked " + handle.key());
        }

        @Override
        public void onBusy(String key, LockOutcome.Busy<?> busy, Duration lockDuration) {
            calls.add("busy " + key + (busy.sameInstance() ? " here" : " elsewhere"));
        }

        @Override
        public void onLockFailed(String key, LockFailedException error, Duration lockDuration) {
            calls.add("failed " + key);
        }

        @Override
        public void onReleased(LockHandle handle, Duration heldDuration) {
            calls.add("released " + handle.key());
        }

        @Override
        public void onConnectionReset() {
            calls.add("reset");
        }
    }

    /** Its connections fail every COMMIT, transiently, so that the lock connection is not re-created. */
    private static ConnectionFactory failingCommits(ConnectionFactory target) {
        return (ConnectionFactory) Proxy.newProxyInstance(ConnectionFactory.class.getClassLoader(), new Class<?>[]{ConnectionFactory.class},
                (_, method, args) -> method.getName().equals("create")
                        ? Mono.<Connection>from(target.create()).map(R2dbcLockManagerTest::failingCommit)
                        : method.invoke(target, args));
    }

    private static Connection failingCommit(Connection target) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (_, method, args) -> method.getName().equals("commitTransaction")
                        ? Mono.error(new R2dbcTransientResourceException("commit failed"))
                        : method.invoke(target, args));
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    private static List<LockOutcome<String>> completed(List<String> keys) {
        return keys.stream().<LockOutcome<String>>map(LockOutcome.Completed::new).toList();
    }

    /** Locks {@code key}, returns it from the locked work and unlocks. */
    private static LockOutcome<String> lockAndUnlock(LockManager lockManager, String key) {
        return await(lockManager.tryWithLock(key, handle -> Mono.just(handle.key())));
    }
}
