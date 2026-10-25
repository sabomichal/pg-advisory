package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockManager.LockHandle;

/**
 * The {@link LockHandle} of a lock taken by {@link R2dbcLockManager}.
 */
record DefaultLockHandle(String key, long fencingToken) implements LockHandle {
}
