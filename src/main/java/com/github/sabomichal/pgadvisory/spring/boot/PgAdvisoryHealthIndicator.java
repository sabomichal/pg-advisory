package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.PgAdvisory;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import reactor.core.publisher.Mono;

/** Up while the lock session is connected. */
public class PgAdvisoryHealthIndicator implements ReactiveHealthIndicator {

    private final PgAdvisory<?> pgAdvisory;

    public PgAdvisoryHealthIndicator(PgAdvisory<?> pgAdvisory) {
        this.pgAdvisory = pgAdvisory;
    }

    @Override
    public Mono<Health> health() {
        return Mono.fromSupplier(() -> {
            var state = pgAdvisory.state();
            var health = state == PgAdvisory.ConnectionState.CONNECTED ? Health.up() : Health.down();
            return health
                    .withDetail("state", state)
                    .withDetail("owner", pgAdvisory.owner())
                    .withDetail("heldLocks", pgAdvisory.heldLocks())
                    .build();
        });
    }
}
