package com.github.sabomichal.pgadvisory;

import io.r2dbc.spi.Connection;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.function.Function;

/**
 * Long-lived distributed locks on PostgreSQL session advisory locks.
 * <p>
 * All locks of one instance are held by a single dedicated connection, the <em>lock session</em>.
 * The work runs on other connections. Every acquisition gets a new, higher <em>fencing token</em> of the key,
 * and work transactions are fenced by {@link LockHandle#verify(Connection)}, so a holder that lost
 * its lock cannot commit after a new holder has started.
 *
 * @param <K> type of the lock key
 */
public interface PgAdvisory<K> {

    /**
     * Acquires the lock for {@code key} without waiting and runs {@code work} while holding it.
     * Uses the default lost-lock policy of this instance.
     *
     * @see #withLock(Object, LostLockPolicy, Function)
     */
    <T> Mono<LockOutcome<T>> withLock(K key, Function<LockHandle, Mono<T>> work);

    /**
     * Acquires the lock for {@code key} without waiting and runs {@code work} while holding it.
     * The lock is released when {@code work} terminates or is cancelled.
     * <p>
     * The {@link LockHandle} is passed to {@code work} and is also available in its Reactor context
     * ({@link LockHandle#current()}). Errors of {@code work} are propagated unchanged, lock related
     * results are returned as {@link LockOutcome}.
     */
    <T> Mono<LockOutcome<T>> withLock(K key, LostLockPolicy lostLockPolicy, Function<LockHandle, Mono<T>> work);

    /**
     * Fencing check for the lock in the current Reactor context, inside the transaction running on
     * {@code connection}. Usually not called directly, see {@code FencingR2dbcTransactionManager}.
     */
    Mono<FenceCheck> fence(Connection connection);

    /** Opens the lock session and verifies (or creates) the lock table. */
    Mono<Void> start();

    /**
     * Stops accepting new locks, waits for running work up to the shutdown timeout, cancels the rest and
     * closes the lock session.
     */
    Mono<Void> stop();

    ConnectionState state();

    /** Number of locks currently held by this instance. */
    int heldLocks();

    /** Owner of this instance, recorded in the lock table. */
    String owner();

    /** Result of {@link #withLock}. */
    sealed interface LockOutcome<T> {

        /** The work finished while the lock was held. */
        record Completed<T>(@Nullable T value, long fencingToken) implements LockOutcome<T> {
        }

        /** Another instance holds the lock; {@code owner} is the owner of its last acquisition, if known. */
        record Busy<T>(@Nullable String owner) implements LockOutcome<T> {
        }

        /**
         * This instance already holds or is acquiring the lock for the key, or for a key with the same
         * {@link KeyMapper#localKey local key}.
         */
        record HeldLocally<T>() implements LockOutcome<T> {
        }

        /** The lock was lost while the work was running; its transactions were fenced off. */
        record Lost<T>(long fencingToken) implements LockOutcome<T> {
        }

        /** The lock could not be attempted: not connected, stopping, or a database error. */
        record Unavailable<T>(@Nullable Throwable cause) implements LockOutcome<T> {
        }
    }

    /** Result of a fencing check inside a work transaction. */
    sealed interface FenceCheck {

        /** The handle's fencing token is still the current one. */
        record Valid() implements FenceCheck {
        }

        /** Another holder took over; {@code current} is the fencing token seen in the database, if known. */
        record Stale(@Nullable Long current) implements FenceCheck {
        }

        /** There is no lock in the current context. */
        record NoLock() implements FenceCheck {
        }
    }

    /** What happens to running work when the lock session is lost. */
    enum LostLockPolicy {
        /** The work is cancelled and {@link #withLock} returns {@link LockOutcome.Lost}. */
        CANCEL_WORK,
        /** The work keeps running; its transactions are still fenced. */
        NOTIFY
    }

    enum ConnectionState {
        STOPPED, STARTING, CONNECTED, RECONNECTING, STOPPING
    }
}
