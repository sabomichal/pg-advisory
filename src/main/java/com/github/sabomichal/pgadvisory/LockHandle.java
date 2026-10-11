package com.github.sabomichal.pgadvisory;

import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

/** A lock held by {@link PgAdvisory#withLock}. */
public interface LockHandle {

    /** Key of the handle in the Reactor context of the work. */
    Object CONTEXT_KEY = LockHandle.class;

    /** The handle of the enclosing {@link PgAdvisory#withLock}, empty outside of it. */
    static Mono<LockHandle> current() {
        return Mono.deferContextual(context -> Mono.justOrEmpty(context.getOrEmpty(CONTEXT_KEY)));
    }

    /** Text form of the key, as stored in the lock table. */
    String key();

    /** Id of the lock table row. */
    long rowId();

    /** Id passed to {@code pg_advisory_lock}. */
    long lockId();

    /** Fencing token of this acquisition, higher than any earlier one of the key. */
    long fencingToken();

    String owner();

    boolean isLost();

    /** Completes when the lock is detected as lost. */
    Mono<Void> lost();

    /**
     * Checks inside the transaction running on {@code connection}, right before its commit, that this
     * handle's fencing token is still the current one. A holder that took over waits for this transaction
     * to commit, so a {@code Valid} result stays valid until then.
     */
    Mono<FenceCheck> verify(Connection connection);
}
