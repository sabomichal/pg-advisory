package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.terminateConnections;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R2dbcLockManagerTest {

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
    void emptyWorkCompletesEmptyAndUnlocks() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();

        assertThat(await(lockManager.tryWithLock(key, _ -> Mono.empty()))).isNull();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
    }

    @Test
    void unreachableDatabaseFailsWithLockFailedException() {
        var lockManager = lockManager(ConnectionFactories.get("r2dbc:postgresql://user:password@localhost:1/db"), settings("A"), () -> {});
        var workRan = new AtomicBoolean();

        assertThatThrownBy(() -> await(lockManager.tryWithLock(newKey(), _ -> Mono.fromRunnable(() -> workRan.set(true)))))
                .isInstanceOf(LockFailedException.class)
                .hasCauseInstanceOf(R2dbcNonTransientResourceException.class);
        assertThat(workRan).isFalse();
    }

    @Test
    void reconnectsWhenTheLockConnectionIsBroken() {
        var resets = new AtomicInteger();
        var applicationName = "lock-manager-" + UUID.randomUUID();
        var lockManager = lockManager(settings(applicationName), resets::incrementAndGet);
        lockAndUnlock(lockManager, newKey());

        assertThat(terminateConnections(applicationName)).isEqualTo(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            await(lockManager.validateDbConnection());
            assertThat(resets).hasValue(1);
        });

        var key = newKey();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(new LockOutcome.Completed<>(key));
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

        assertThat(outcomes).containsExactlyInAnyOrderElementsOf(completed(keys));
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(120));
    }

    // ------------------------------------------------------------------ helpers

    private R2dbcLockManager lockManager(LockManagerSettings.Builder settings) {
        return lockManager(settings, () -> {});
    }

    private R2dbcLockManager lockManager(LockManagerSettings.Builder settings, Runnable onConnectionReset) {
        return lockManager(TestPostgres.connectionFactory(), settings, onConnectionReset);
    }

    private R2dbcLockManager lockManager(ConnectionFactory connectionFactory, LockManagerSettings.Builder settings,
                                         Runnable onConnectionReset) {
        var lockManager = new R2dbcLockManager(connectionFactory, settings.build(), onConnectionReset::run);
        lockManagers.add(lockManager);
        return lockManager;
    }

    private static LockManagerSettings.Builder settings(String applicationName) {
        // like the original LockManagerTest
        return LockManagerSettings.builder().applicationName(applicationName).retryMaxBackoff(Duration.ofMillis(1000));
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
