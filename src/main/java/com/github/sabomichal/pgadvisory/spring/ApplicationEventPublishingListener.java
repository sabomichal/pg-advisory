package com.github.sabomichal.pgadvisory.spring;

import com.github.sabomichal.pgadvisory.AdvisoryLockListener;
import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome;
import org.jspecify.annotations.Nullable;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;

/** Publishes the callbacks as {@link PgAdvisoryEvents}. */
public class ApplicationEventPublishingListener implements AdvisoryLockListener {

    private final ApplicationEventPublisher publisher;

    public ApplicationEventPublishingListener(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void onAcquired(LockHandle handle, Duration acquireDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.LockAcquired(handle, acquireDuration));
    }

    @Override
    public void onAcquireFailed(String key, LockOutcome<?> outcome, Duration acquireDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.LockAcquireFailed(key, outcome, acquireDuration));
    }

    @Override
    public void onReleased(LockHandle handle, Duration heldDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.LockReleased(handle, heldDuration));
    }

    @Override
    public void onLost(LockHandle handle) {
        publisher.publishEvent(new PgAdvisoryEvents.LockLost(handle));
    }

    @Override
    public void onConnectionLost(@Nullable Throwable cause) {
        publisher.publishEvent(new PgAdvisoryEvents.ConnectionLost(cause));
    }

    @Override
    public void onConnectionRestored() {
        publisher.publishEvent(new PgAdvisoryEvents.ConnectionRestored());
    }
}
