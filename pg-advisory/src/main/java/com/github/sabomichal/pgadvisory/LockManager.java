package com.github.sabomichal.pgadvisory;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Distributed locks on PostgreSQL session advisory locks, all held by one dedicated connection.
 */
public interface LockManager {

    /**
     * Locks {@code key} without waiting, runs {@code work} and unlocks. Empty when the work completes empty.
     * {@link LockFailedException}: not locked because of an error, the work did not run. Other errors pass unchanged.
     */
    <T> Mono<LockOutcome<T>> tryWithLock(
            String key, Function<LockHandle, Mono<T>> work
    );

    /**
     * Like {@link #tryWithLock(String, Function)}, with callbacks right around {@code work}, not called when busy.
     */
    <T> Mono<LockOutcome<T>> tryWithLock(
            String key, Consumer<LockHandle> onLock, Consumer<LockHandle> onFinally, Function<LockHandle, Mono<T>> work
    );

    /**
     * Result of {@link #tryWithLock}.
     */
    sealed interface LockOutcome<T> {

        /**
         * The work finished under the lock with {@code value}.
         */
        record Completed<T>(T value) implements LockOutcome<T> {
        }

        /**
         * Locked by this instance when {@code sameInstance}, otherwise by another one. The work did not run.
         */
        record Busy<T>(boolean sameInstance) implements LockOutcome<T> {
        }
    }

    /**
     * A lock held by {@link #tryWithLock}, passed to its work and callbacks.
     */
    interface LockHandle {

        /**
         * Key of the handle in the Reactor context of the work.
         */
        Object CONTEXT_KEY = LockHandle.class;

        /**
         * The handle of the enclosing {@link #tryWithLock}, empty outside of it.
         */
        static Mono<LockHandle> current() {
            return Mono.deferContextual(context -> Mono.justOrEmpty(context.getOrEmpty(CONTEXT_KEY)));
        }

        /**
         * The key passed to {@link #tryWithLock}.
         */
        String key();

        /**
         * Fencing token of this lock, higher than the token of every earlier lock of the key, by any instance. Pass it
         * along with writes, so that their target rejects writes with an older token of the key.
         */
        long fencingToken();
    }
}
