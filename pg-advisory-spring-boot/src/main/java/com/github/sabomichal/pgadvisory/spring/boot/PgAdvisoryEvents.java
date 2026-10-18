package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerListener;

import java.time.Duration;

/**
 * The {@link LockManagerListener} callbacks as Spring application events, published synchronously on the locking path.
 * An {@code @EventListener} of {@code PgAdvisoryEvents} gets all of them and can handle them in an exhaustive switch.
 */
public sealed interface PgAdvisoryEvents {

    /** See {@link LockManagerListener#onLocked}. */
    record Locked(LockHandle handle, Duration lockDuration) implements PgAdvisoryEvents {
    }

    /** See {@link LockManagerListener#onBusy}. */
    record Busy(String key, LockOutcome.Busy<?> busy, Duration lockDuration) implements PgAdvisoryEvents {
    }

    /** See {@link LockManagerListener#onLockFailed}. */
    record LockFailed(String key, LockFailedException error, Duration lockDuration) implements PgAdvisoryEvents {
    }

    /** See {@link LockManagerListener#onReleased}. */
    record Released(LockHandle handle, Duration heldDuration) implements PgAdvisoryEvents {
    }

    /** See {@link LockManagerListener#onConnectionReset}. */
    record ConnectionReset() implements PgAdvisoryEvents {
    }
}
