package com.github.sabomichal.pgadvisory;

/** The lock session is not connected: {@link PgAdvisory} is stopped, starting, stopping or reconnecting. */
public class LockSessionUnavailableException extends IllegalStateException {

    public LockSessionUnavailableException() {
        super("Advisory lock session is not connected");
    }
}
