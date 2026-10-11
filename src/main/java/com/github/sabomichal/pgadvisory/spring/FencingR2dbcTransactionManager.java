package com.github.sabomichal.pgadvisory.spring;

import com.github.sabomichal.pgadvisory.AdvisoryLockFencedException;
import com.github.sabomichal.pgadvisory.LockHandle;
import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.r2dbc.connection.ConnectionHolder;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.reactive.GenericReactiveTransaction;
import org.springframework.transaction.reactive.TransactionSynchronizationManager;
import reactor.core.publisher.Mono;

/**
 * Fences every transaction started inside {@code PgAdvisory.withLock}: right before {@code COMMIT} it checks
 * on the transaction's connection that the handle's fencing token is still the current one. A stale token
 * rolls the transaction back with {@link AdvisoryLockFencedException}, which {@code withLock} reports as
 * {@code Lost}.
 * <p>
 * Fencing protects writes: read-only transactions are not checked.
 */
public class FencingR2dbcTransactionManager extends R2dbcTransactionManager {

    public FencingR2dbcTransactionManager(ConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    @Override
    protected Mono<Void> prepareForCommit(TransactionSynchronizationManager synchronizationManager,
                                          GenericReactiveTransaction status) {
        return super.prepareForCommit(synchronizationManager, status)
                .then(LockHandle.current())
                .filter(_ -> status.isNewTransaction() && !status.isReadOnly())
                .flatMap(handle -> verify(handle, synchronizationManager));
    }

    private Mono<Void> verify(LockHandle handle, TransactionSynchronizationManager synchronizationManager) {
        var holder = (ConnectionHolder) synchronizationManager.getResource(obtainConnectionFactory());
        if (holder == null) {
            return Mono.empty();
        }
        return handle.verify(holder.getConnection()).flatMap(check -> check instanceof FenceCheck.Valid
                ? Mono.empty()
                : Mono.error(new AdvisoryLockFencedException(handle, check)));
    }
}
