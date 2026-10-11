package com.github.sabomichal.pgadvisory;

import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;

/**
 * Fails the commit of a work transaction whose lock was taken over.
 * {@link PgAdvisory#withLock} reports it as {@code LockOutcome.Lost}.
 */
public class AdvisoryLockFencedException extends RuntimeException {

    private final transient LockHandle handle;
    private final transient FenceCheck check;

    public AdvisoryLockFencedException(LockHandle handle, FenceCheck check) {
        super("Advisory lock [%s] fencing token [%d] was fenced off: %s".formatted(handle.key(), handle.fencingToken(), check));
        this.handle = handle;
        this.check = check;
    }

    public LockHandle handle() {
        return handle;
    }

    public FenceCheck check() {
        return check;
    }
}
