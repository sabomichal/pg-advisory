package com.github.sabomichal.pgadvisory;

import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

/**
 * Writes additional columns of the lock table row, e.g. an endpoint where the holder can be reached.
 * Runs in the takeover transaction on a work connection, right after the fencing token was issued; a failure
 * rolls the transaction back and the acquisition ends {@code Unavailable}.
 */
@FunctionalInterface
public interface LockRecordWriter {

    Mono<Void> onAcquire(Connection connection, String tableName, long rowId, String key);
}
