package com.github.sabomichal.pgadvisory;

/**
 * Called when the lock connection was found broken and is re-created. All its locks are gone, so work running
 * under them should stop.
 */
@FunctionalInterface
public interface ConnectionResetListener {

    void onConnectionReset();
}
