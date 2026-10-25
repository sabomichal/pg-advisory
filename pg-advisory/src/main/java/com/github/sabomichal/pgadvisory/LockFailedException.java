package com.github.sabomichal.pgadvisory;

/**
 * The key could not be locked because of an error, e.g. an unreachable database, or its fencing token could not be
 * issued, then the lock was released first. The work did not run.
 */
public class LockFailedException extends RuntimeException {

    public LockFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
