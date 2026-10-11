package com.github.sabomichal.pgadvisory.micrometer;

import com.github.sabomichal.pgadvisory.AdvisoryLockListener;
import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.jspecify.annotations.Nullable;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Metrics: {@code pg.advisory.acquire} (timer, tag {@code outcome}), {@code pg.advisory.hold} (timer),
 * {@code pg.advisory.locks.held} (gauge), {@code pg.advisory.lost} and {@code pg.advisory.connection.lost} (counters).
 */
public class MicrometerAdvisoryLockListener implements AdvisoryLockListener {

    private final MeterRegistry registry;
    private final AtomicInteger held = new AtomicInteger();
    private final Timer holdTimer;
    private final Counter lostCounter;
    private final Counter connectionLostCounter;

    public MicrometerAdvisoryLockListener(MeterRegistry registry) {
        this.registry = registry;
        this.holdTimer = Timer.builder("pg.advisory.hold").description("Time an advisory lock was held").register(registry);
        this.lostCounter = Counter.builder("pg.advisory.lost").register(registry);
        this.connectionLostCounter = Counter.builder("pg.advisory.connection.lost").register(registry);
        Gauge.builder("pg.advisory.locks.held", held, AtomicInteger::get).register(registry);
    }

    @Override
    public void onAcquired(LockHandle handle, Duration acquireDuration) {
        held.incrementAndGet();
        acquireTimer("acquired").record(acquireDuration);
    }

    @Override
    public void onAcquireFailed(String key, LockOutcome<?> outcome, Duration acquireDuration) {
        var tag = switch (outcome) {
            case LockOutcome.Busy<?> busy -> "busy";
            case LockOutcome.HeldLocally<?> heldLocally -> "held_locally";
            default -> "unavailable";
        };
        acquireTimer(tag).record(acquireDuration);
    }

    @Override
    public void onReleased(LockHandle handle, Duration heldDuration) {
        held.decrementAndGet();
        holdTimer.record(heldDuration);
    }

    @Override
    public void onLost(LockHandle handle) {
        lostCounter.increment();
    }

    @Override
    public void onConnectionLost(@Nullable Throwable cause) {
        connectionLostCounter.increment();
    }

    private Timer acquireTimer(String outcome) {
        return Timer.builder("pg.advisory.acquire").tag("outcome", outcome).register(registry);
    }
}
