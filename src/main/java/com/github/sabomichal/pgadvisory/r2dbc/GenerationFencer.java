package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.LockSessionUnavailableException;
import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Mono;

import java.time.Duration;

/**
 * {@link Fencer} whose fencing token is the {@code generation} column of the key's row in the lock table.
 * The two-int advisory lock {@code (barrierNamespace, barrierKey)} is a barrier between the old and the new holder.
 * <ul>
 *   <li>{@link #verify} takes the barrier <em>shared</em>, held until commit, and only then reads the generation.</li>
 *   <li>{@link #takeOver} takes the barrier <em>exclusively</em>, increments the generation and, still holding
 *       the barrier, has the lock session confirmed, in one transaction on a work connection under
 *       {@code lock_timeout}.</li>
 * </ul>
 * So every check is either after a takeover (it sees the new generation and rolls back), or before it (the new
 * holder waits until this transaction has committed).
 * <p>
 * Under READ COMMITTED the check reads the generation without locking the row, so it writes nothing: an
 * advisory lock lives only in the lock table in memory. Under REPEATABLE READ and SERIALIZABLE the snapshot may
 * predate a takeover, so the generation is read {@code FOR SHARE} (see {@link #lockingRead}). That row lock is
 * held until commit and makes the new holder's {@code UPDATE} wait as well; this is why the increment runs on
 * a work connection under {@code lock_timeout} and never on the lock session, whose work is serialized.
 */
final class GenerationFencer implements Fencer {

    private final LockStore store;
    private final int barrierNamespace;

    GenerationFencer(LockStore store, int barrierNamespace) {
        this.store = store;
        this.barrierNamespace = barrierNamespace;
    }

    @Override
    public Mono<Long> takeOver(Connection connection, SessionLock lock, String owner, Duration timeout, Mono<Boolean> lockStillHeld) {
        return store.setLockTimeout(connection, timeout)
                .then(store.takeExclusiveBarrier(connection, barrierNamespace, barrierKey(lock.lockId())))
                .then(store.nextGeneration(connection, lock.rowId(), owner))
                // under the exclusive barrier, which a later holder has to pass as well
                .flatMap(next -> lockStillHeld.flatMap(held -> held
                        ? Mono.just(next.generation())
                        : Mono.error(new LockSessionUnavailableException())));
    }

    @Override
    public Mono<FenceCheck> verify(Connection connection, LockHandle handle) {
        return store.takeSharedBarrier(connection, barrierNamespace, barrierKey(handle.lockId()))
                .then(store.readGeneration(connection, handle.rowId()))
                .flatMap(read -> read.readCommitted()
                        ? Mono.just(read.generation())
                        : lockingRead(connection, handle.rowId()))
                .map(current -> current == handle.fencingToken() ? (FenceCheck) new FenceCheck.Valid() : new FenceCheck.Stale(current))
                .defaultIfEmpty(new FenceCheck.Stale(null));
    }

    /**
     * Under REPEATABLE READ and SERIALIZABLE the snapshot may predate a takeover. A locking read of a row
     * changed after the snapshot fails with a serialization error, which means the generation is stale.
     */
    private Mono<Long> lockingRead(Connection connection, long rowId) {
        return store.readGenerationForShare(connection, rowId)
                .onErrorResume(error -> SqlStates.is(error, SqlStates.SERIALIZATION_FAILURE), _ -> Mono.empty());
    }

    /** Folds the 64-bit lock id into the 32-bit second key of the barrier; collisions only cause extra waiting. */
    private static int barrierKey(long lockId) {
        return Long.hashCode(lockId);
    }
}
