package com.github.sabomichal.pgadvisory.r2dbc;

/**
 * An advisory lock taken on the lock session, before its holder got a fencing token.
 *
 * @param incarnation the lock session incarnation that holds the lock
 */
record SessionLock(String key, long rowId, long lockId, long incarnation) {
}
