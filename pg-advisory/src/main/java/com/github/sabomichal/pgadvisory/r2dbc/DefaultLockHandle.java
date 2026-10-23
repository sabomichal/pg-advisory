package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockManager.LockHandle;

record DefaultLockHandle(String key, long fencingToken) implements LockHandle {
}
