package com.github.sabomichal.pgadvisory;

/** The key could not be locked: it is held by another instance, or by this one when {@code sameInstance}. */
public class LockOperationException extends RuntimeException {

    private final boolean sameInstance;

    public LockOperationException(String message) {
        this(message, false);
    }

    public LockOperationException(String message, boolean sameInstance) {
        super(message);
        this.sameInstance = sameInstance;
    }

    public boolean isSameInstance() {
        return sameInstance;
    }
}
