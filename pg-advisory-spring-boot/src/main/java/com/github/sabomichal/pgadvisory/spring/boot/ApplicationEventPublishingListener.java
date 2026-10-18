package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import org.springframework.context.ApplicationEventPublisher;

import java.time.Duration;

/** Publishes the callbacks as {@link PgAdvisoryEvents}. */
public class ApplicationEventPublishingListener implements LockManagerListener {

    private final ApplicationEventPublisher publisher;

    public ApplicationEventPublishingListener(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void onLocked(LockHandle handle, Duration lockDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.Locked(handle, lockDuration));
    }

    @Override
    public void onBusy(String key, LockOutcome.Busy<?> busy, Duration lockDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.Busy(key, busy, lockDuration));
    }

    @Override
    public void onLockFailed(String key, LockFailedException error, Duration lockDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.LockFailed(key, error, lockDuration));
    }

    @Override
    public void onReleased(LockHandle handle, Duration heldDuration) {
        publisher.publishEvent(new PgAdvisoryEvents.Released(handle, heldDuration));
    }

    @Override
    public void onConnectionReset() {
        publisher.publishEvent(new PgAdvisoryEvents.ConnectionReset());
    }
}
