package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockManager;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import reactor.core.publisher.Mono;

/** Up when the lock connection passes {@link LockManager#validateDbConnection()}. */
public class PgAdvisoryHealthIndicator implements ReactiveHealthIndicator {

    private final LockManager lockManager;

    public PgAdvisoryHealthIndicator(LockManager lockManager) {
        this.lockManager = lockManager;
    }

    @Override
    public Mono<Health> health() {
        return lockManager.validateDbConnection()
                .then(Mono.fromSupplier(() -> Health.up().build()))
                .onErrorResume(error -> Mono.just(Health.down(error).build()));
    }
}
