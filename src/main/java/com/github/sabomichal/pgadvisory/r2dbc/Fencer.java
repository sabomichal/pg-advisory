package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * Fencing: a holder that lost its lock must not commit writes after a new holder took over.
 * <p>
 * Every acquisition of a key gets a <em>fencing token</em>, higher than any earlier one of the key:
 * <ol>
 *   <li>{@link #takeOver}: after the advisory lock was taken on the lock session, the new holder waits until
 *       transactions that passed their check with an older token have committed, then gets its token. From
 *       then on, checks of older tokens fail. Only then its work starts.</li>
 *   <li>{@link #verify}: every work transaction checks its token right before commit.</li>
 * </ol>
 */
interface Fencer {

    /**
     * Runs in a transaction on a work connection, never on the lock session, so that waiting for the previous
     * holder blocks nothing else. Fails with {@code lock_not_available} when the previous holder's transactions
     * have not committed within {@code timeout}; the caller rolls back and releases the lock.
     * <p>
     * Taking the advisory lock and issuing the token happen on different connections, so the lock may be lost
     * in between, and another holder may already have taken over. Hence the invariant: the token is issued only
     * if {@code lockStillHeld} confirms, while this transaction holds what keeps the next holder out, that the
     * lock session still holds the lock; otherwise this fails with {@code LockSessionUnavailableException}. A loss
     * after the confirmation is harmless: the next holder waits for this transaction and issues a higher token.
     *
     * @return the new fencing token
     */
    Mono<Long> takeOver(Connection connection, SessionLock lock, String owner, Duration timeout, Mono<Boolean> lockStillHeld);

    /** Runs in a work transaction right before its commit: whether the handle's token is still the current one. */
    Mono<FenceCheck> verify(Connection connection, LockHandle handle);
}
