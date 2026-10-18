package com.github.sabomichal.pgadvisory;

/** The key could not be locked because of an error, e.g. an unreachable database. The work did not run. */
public class LockFailedException extends RuntimeException {

    public LockFailedException(String message, Throwable cause) {
        super(message, cause);
    }
}
