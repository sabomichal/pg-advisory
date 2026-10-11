package com.github.sabomichal.pgadvisory.spring;

import com.github.sabomichal.pgadvisory.PgAdvisory;
import org.springframework.context.SmartLifecycle;

/**
 * Opens the lock session when the context starts; on stop drains running work, releases the locks and
 * closes the session. Also covers CRaC checkpoint/restore.
 */
public class PgAdvisoryLifecycle implements SmartLifecycle {

    private final PgAdvisory<?> pgAdvisory;
    private final int phase;

    public PgAdvisoryLifecycle(PgAdvisory<?> pgAdvisory, int phase) {
        this.pgAdvisory = pgAdvisory;
        this.phase = phase;
    }

    @Override
    public void start() {
        pgAdvisory.start().block();
    }

    @Override
    public void stop() {
        pgAdvisory.stop().block();
    }

    @Override
    public void stop(Runnable callback) {
        pgAdvisory.stop().doFinally(_ -> callback.run()).subscribe();
    }

    @Override
    public boolean isRunning() {
        return pgAdvisory.state() != PgAdvisory.ConnectionState.STOPPED;
    }

    @Override
    public int getPhase() {
        return phase;
    }
}
