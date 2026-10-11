package com.github.sabomichal.pgadvisory.spring;

import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome;
import org.jspecify.annotations.Nullable;

import java.time.Duration;

/**
 * Spring application events published by {@link ApplicationEventPublishingListener}. Being sealed, a listener
 * of {@code PgAdvisoryEvents} receives all of them and can handle them in an exhaustive {@code switch}.
 */
public sealed interface PgAdvisoryEvents {

    record LockAcquired(LockHandle handle, Duration acquireDuration) implements PgAdvisoryEvents {
    }

    record LockAcquireFailed(String key, LockOutcome<?> outcome, Duration acquireDuration) implements PgAdvisoryEvents {
    }

    record LockReleased(LockHandle handle, Duration heldDuration) implements PgAdvisoryEvents {
    }

    record LockLost(LockHandle handle) implements PgAdvisoryEvents {
    }

    record ConnectionLost(@Nullable Throwable cause) implements PgAdvisoryEvents {
    }

    record ConnectionRestored() implements PgAdvisoryEvents {
    }
}
