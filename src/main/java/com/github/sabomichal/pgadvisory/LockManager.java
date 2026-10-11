package com.github.sabomichal.pgadvisory;

import reactor.core.publisher.Mono;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Distributed locks on PostgreSQL session advisory locks, held by one dedicated connection.
 */
public interface LockManager {

    /**
     * Locks {@code key} without waiting, runs {@code function} and unlocks.
     * Fails with {@link LockOperationException} when the key is locked, by this or another instance.
     */
    <T> Mono<T> tryWithLock(String key, Function<String, Mono<T>> function);

    /** Like {@link #tryWithLock(String, Function)}, with callbacks right before and after {@code function}. */
    <T> Mono<T> tryWithLock(String key, Consumer<String> onLock, Consumer<String> onFinally, Function<String, Mono<T>> function);

    /** Checks the lock connection and re-creates it when it is broken; its locks are gone then. */
    Mono<Void> validateDbConnection();

    Mono<String> unlock(String key);
}
