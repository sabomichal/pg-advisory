package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.concurrent.atomic.AtomicReference;

final class DefaultLockHandle implements LockHandle {

    private enum State { HELD, LOST, RELEASED }

    private final String key;
    private final long rowId;
    private final long lockId;
    private final long fencingToken;
    private final String owner;
    private final long incarnation;
    private final Fencer fencer;
    private final long acquiredNanos = System.nanoTime();
    private final AtomicReference<State> state = new AtomicReference<>(State.HELD);
    private final Sinks.One<Boolean> lostSignal = Sinks.one();

    DefaultLockHandle(String key, long rowId, long lockId, long fencingToken, String owner, long incarnation,
                      Fencer fencer) {
        this.key = key;
        this.rowId = rowId;
        this.lockId = lockId;
        this.fencingToken = fencingToken;
        this.owner = owner;
        this.incarnation = incarnation;
        this.fencer = fencer;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public long rowId() {
        return rowId;
    }

    @Override
    public long lockId() {
        return lockId;
    }

    @Override
    public long fencingToken() {
        return fencingToken;
    }

    @Override
    public String owner() {
        return owner;
    }

    @Override
    public boolean isLost() {
        return state.get() == State.LOST;
    }

    @Override
    public Mono<Void> lost() {
        return lostSignal.asMono().then();
    }

    @Override
    public Mono<FenceCheck> verify(Connection connection) {
        return fencer.verify(connection, this);
    }

    long incarnation() {
        return incarnation;
    }

    long acquiredNanos() {
        return acquiredNanos;
    }

    boolean isHeld() {
        return state.get() == State.HELD;
    }

    /** Emits once the lock is lost; for {@code takeUntilOther}. */
    Mono<Boolean> lostSignal() {
        return lostSignal.asMono();
    }

    /** @return true if this call changed the state to lost */
    boolean markLost() {
        if (state.compareAndSet(State.HELD, State.LOST)) {
            lostSignal.tryEmitValue(true);
            return true;
        }
        return false;
    }

    void markReleased() {
        state.compareAndSet(State.HELD, State.RELEASED);
    }

    @Override
    public String toString() {
        return "LockHandle[key=%s, lockId=%d, fencingToken=%d, state=%s]".formatted(key, lockId, fencingToken, state.get());
    }
}
