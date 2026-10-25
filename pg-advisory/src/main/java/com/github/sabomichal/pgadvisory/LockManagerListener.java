package com.github.sabomichal.pgadvisory;

import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;

import java.time.Duration;

/**
 * Callbacks about locks and the lock connection. They run on the locking path, so they must be fast and must not
 * block. {@code lockDuration} includes waiting for the lock connection.
 */
public interface LockManagerListener {

    /**
     * The key was locked, the work starts next unless its caller cancelled meanwhile. Followed by exactly one {@link #onReleased}.
     */
    default void onLocked(LockHandle handle, Duration lockDuration) {
    }

    /**
     * The key is locked by this or another instance, see {@link LockOutcome.Busy}. The work does not run.
     */
    default void onBusy(String key, LockOutcome.Busy<?> busy, Duration lockDuration) {
    }

    /**
     * The key could not be locked, {@code tryWithLock} fails with the same {@code error}. The work does not run.
     */
    default void onLockFailed(String key, LockFailedException error, Duration lockDuration) {
    }

    /**
     * After the lock was released: unlocked, a failed unlock repeated until then, or gone with its lock connection.
     */
    default void onReleased(LockHandle handle, Duration heldDuration) {
    }

    /**
     * The lock connection was found broken and re-created. All its locks are gone, work under them should stop.
     */
    default void onConnectionReset() {
    }
}
