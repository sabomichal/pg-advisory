package com.github.sabomichal.pgadvisory;

import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

/** Callbacks about locks and the lock session. Implementations must be fast and must not block. */
public interface AdvisoryLockListener {

    default void onAcquired(LockHandle handle, Duration acquireDuration) {
    }

    /** The lock was not acquired: {@code outcome} is {@code Busy}, {@code HeldLocally} or {@code Unavailable}. */
    default void onAcquireFailed(String key, LockOutcome<?> outcome, Duration acquireDuration) {
    }

    default void onReleased(LockHandle handle, Duration heldDuration) {
    }

    default void onLost(LockHandle handle) {
    }

    default void onConnectionLost(@Nullable Throwable cause) {
    }

    default void onConnectionRestored() {
    }
}
