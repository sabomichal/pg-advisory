package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.LockOperationException;
import com.github.sabomichal.pgadvisory.TestPostgres;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
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
import java.util.concurrent.atomic.AtomicInteger;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.terminateConnections;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class R2dbcLockManagerTest {

    private final List<R2dbcLockManager> lockManagers = new ArrayList<>();

    @BeforeAll
    void createLockTable() {
        await(lockManager(settings("setup").autoCreateSchema(true)).ensureTable());
    }

    @AfterEach
    void closeLockManagers() {
        lockManagers.forEach(R2dbcLockManager::close);
        lockManagers.clear();
    }

    @Test
    void locksMultipleKeys() {
        var lockManager = lockManager(settings("A"));
        var keys = Flux.range(0, 10).map(_ -> newKey()).collectList().block();

        var locked = await(Flux.fromIterable(keys)
                .flatMap(key -> lockManager.tryWithLock(key, lockedKey -> Mono.just(lockedKey).delayElement(Duration.ofMillis(10))))
                .collectList());

        assertThat(locked).containsExactlyInAnyOrderElementsOf(keys);
    }

    @Test
    void lockIsNotReentrant() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();
        var holder = hold(lockManager, key);

        assertThatThrownBy(() -> lockAndUnlock(lockManager, key))
                .isInstanceOfSatisfying(LockOperationException.class, error -> assertThat(error.isSameInstance()).isTrue());
        var other = newKey();
        assertThat(lockAndUnlock(lockManager, other)).isEqualTo(other);

        holder.release();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(key);
    }

    @Test
    void anotherInstanceCannotLockTheKey() {
        var a = lockManager(settings("A"));
        var b = lockManager(settings("B"));
        var key = newKey();
        var holder = hold(a, key);

        assertThatThrownBy(() -> lockAndUnlock(b, key))
                .isInstanceOfSatisfying(LockOperationException.class, error -> assertThat(error.isSameInstance()).isFalse());

        holder.release();
        assertThat(lockAndUnlock(b, key)).isEqualTo(key);
    }

    @Test
    void callsOnLockBeforeAndOnFinallyAfterTheFunction() {
        var lockManager = lockManager(settings("A"));
        var calls = new CopyOnWriteArrayList<String>();

        await(lockManager.tryWithLock(newKey(), _ -> calls.add("lock"), _ -> calls.add("finally"),
                key -> Mono.fromRunnable(() -> calls.add("function")).thenReturn(key)));

        assertThat(calls).containsExactly("lock", "function", "finally");
    }

    @Test
    void unlockOfAKeyThatIsNotLockedReturnsTheKey() {
        var lockManager = lockManager(settings("A"));
        var key = newKey();
        lockAndUnlock(lockManager, key);

        assertThat(await(lockManager.unlock(key))).isEqualTo(key);
    }

    @Test
    void reconnectsWhenTheLockConnectionIsBroken() {
        var resets = new AtomicInteger();
        var applicationName = "lock-manager-" + UUID.randomUUID();
        var lockManager = lockManager(settings(applicationName), resets::incrementAndGet);
        await(lockManager.tryWithLock(newKey(), Mono::just));

        assertThat(terminateConnections(applicationName)).isEqualTo(1);
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            await(lockManager.validateDbConnection());
            assertThat(resets).hasValue(1);
        });

        var key = newKey();
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(key);
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
        assertThat(lockAndUnlock(lockManager, key)).isEqualTo(key);
    }

    @Test
    void locksManyKeysInTime() {
        var lockManager = lockManager(settings("A"));
        var keys = Flux.range(0, 1000).map(_ -> newKey()).collectList().block();

        var started = System.nanoTime();
        var locked = Flux.fromIterable(keys)
                .flatMap(key -> lockManager.tryWithLock(key, Mono::just))
                .collectList()
                .block(Duration.ofSeconds(120));

        assertThat(locked).hasSize(keys.size());
        assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(120));
    }

    // ------------------------------------------------------------------ helpers

    private R2dbcLockManager lockManager(LockManagerSettings.Builder settings) {
        return lockManager(settings, () -> {
        });
    }

    private R2dbcLockManager lockManager(LockManagerSettings.Builder settings, Runnable onConnectionReset) {
        var lockManager = new R2dbcLockManager(TestPostgres.connectionFactory(), settings.build(), onConnectionReset::run);
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
        var outcome = lockManager.tryWithLock(key, lockedKey -> {
            locked.complete(null);
            return Mono.fromFuture(release).thenReturn(lockedKey);
        }).toFuture();
        locked.join();
        return new Holder(release, outcome);
    }

    private record Holder(CompletableFuture<Void> releaseSignal, CompletableFuture<String> outcome) {
        void release() {
            releaseSignal.complete(null);
            outcome.join();
        }
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    /** Locks {@code key}, returns it from the locked function and unlocks. */
    private static String lockAndUnlock(LockManager lockManager, String key) {
        return await(lockManager.tryWithLock(key, Mono::just));
    }
}
