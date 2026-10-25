package com.github.sabomichal.pgadvisory.micrometer;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

public class MicrometerLockManagerListener implements LockManagerListener {

    private final AtomicInteger held = new AtomicInteger();
    private final Timer locked;
    private final Timer busySameInstance;
    private final Timer busyOtherInstance;
    private final Timer failed;
    private final Timer hold;
    private final Counter connectionResets;

    public MicrometerLockManagerListener(MeterRegistry registry) {
        // Registers all meters at once, so that they show up before the first lock.
        this.locked = lockTimer(registry, "locked");
        this.busySameInstance = lockTimer(registry, "busy_same_instance");
        this.busyOtherInstance = lockTimer(registry, "busy_other_instance");
        this.failed = lockTimer(registry, "failed");
        this.hold = Timer.builder("pg.advisory.hold").description("Time a lock was held").register(registry);
        this.connectionResets = Counter.builder("pg.advisory.connection.resets")
                .description("Re-creations of a broken lock connection, which drop all its locks")
                .register(registry);
        Gauge.builder("pg.advisory.locks.held", held, AtomicInteger::get).description("Locks held now").register(registry);
    }

    @Override
    public void onLocked(LockHandle handle, Duration lockDuration) {
        held.incrementAndGet();
        locked.record(lockDuration);
    }

    @Override
    public void onBusy(String key, LockOutcome.Busy<?> busy, Duration lockDuration) {
        (busy.sameInstance() ? busySameInstance : busyOtherInstance).record(lockDuration);
    }

    @Override
    public void onLockFailed(String key, LockFailedException error, Duration lockDuration) {
        failed.record(lockDuration);
    }

    @Override
    public void onReleased(LockHandle handle, Duration heldDuration) {
        held.decrementAndGet();
        hold.record(heldDuration);
    }

    @Override
    public void onConnectionReset() {
        connectionResets.increment();
    }

    private static Timer lockTimer(MeterRegistry registry, String outcome) {
        return Timer.builder("pg.advisory.lock")
                .description("Time to lock a key, including the wait for the lock connection")
                .tag("outcome", outcome)
                .register(registry);
    }
}
