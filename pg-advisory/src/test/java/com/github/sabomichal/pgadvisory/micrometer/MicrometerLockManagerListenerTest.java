package com.github.sabomichal.pgadvisory.micrometer;

import com.github.sabomichal.pgadvisory.LockFailedException;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class MicrometerLockManagerListenerTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MicrometerLockManagerListener listener = new MicrometerLockManagerListener(registry);
    private final LockHandle handle = new Handle("key", 1);

    @Test
    void recordsTheLockTimeByOutcome() {
        listener.onLocked(handle, Duration.ofMillis(10));
        listener.onBusy("key", new LockOutcome.Busy<>(true), Duration.ofMillis(20));
        listener.onBusy("key", new LockOutcome.Busy<>(false), Duration.ofMillis(30));
        listener.onBusy("key", new LockOutcome.Busy<>(false), Duration.ofMillis(30));
        listener.onLockFailed("key", new LockFailedException("failed", new IllegalStateException()), Duration.ofMillis(40));

        assertThat(lockTimer("locked").totalTime(TimeUnit.MILLISECONDS)).isEqualTo(10);
        assertThat(lockTimer("busy_same_instance").count()).isEqualTo(1);
        assertThat(lockTimer("busy_other_instance").count()).isEqualTo(2);
        assertThat(lockTimer("failed").totalTime(TimeUnit.MILLISECONDS)).isEqualTo(40);
    }

    @Test
    void countsHeldLocksAndRecordsTheHoldTime() {
        listener.onLocked(handle, Duration.ZERO);
        listener.onLocked(handle, Duration.ZERO);
        assertThat(registry.get("pg.advisory.locks.held").gauge().value()).isEqualTo(2);

        listener.onReleased(handle, Duration.ofMillis(50));

        assertThat(registry.get("pg.advisory.locks.held").gauge().value()).isEqualTo(1);
        assertThat(registry.get("pg.advisory.hold").timer().totalTime(TimeUnit.MILLISECONDS)).isEqualTo(50);
    }

    @Test
    void countsConnectionResets() {
        listener.onConnectionReset();

        assertThat(registry.get("pg.advisory.connection.resets").counter().count()).isEqualTo(1);
    }

    @Test
    void registersAllOutcomesUpFront() {
        assertThat(registry.get("pg.advisory.lock").timers())
                .extracting(timer -> timer.getId().getTag("outcome"))
                .containsExactlyInAnyOrder("locked", "busy_same_instance", "busy_other_instance", "failed");
    }

    private Timer lockTimer(String outcome) {
        return registry.get("pg.advisory.lock").tag("outcome", outcome).timer();
    }

    private record Handle(String key, long fencingToken) implements LockHandle {
    }
}
