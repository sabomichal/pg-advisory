package com.github.sabomichal.pgadvisory.r2dbc;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * One shared connection, created on first use and kept until {@link #destroy()}. Callers must not close it.
 * Like Spring's {@code SingleConnectionFactory} with auto-commit, without depending on Spring.
 */
final class SingleConnectionFactory {

    private final AtomicReference<@Nullable Connection> created = new AtomicReference<>();
    private final Mono<Connection> connection;

    /**
     * @param initializer runs once on the new connection
     */
    SingleConnectionFactory(ConnectionFactory target, Function<Connection, Mono<Void>> initializer) {
        this.connection = Mono.<Connection>from(target.create())
                .doOnNext(created::set)
                .flatMap(newConnection -> initializer.apply(newConnection).thenReturn(newConnection))
                .cache();
    }

    Mono<Connection> create() {
        return connection.flatMap(shared -> Mono.from(shared.setAutoCommit(true)).thenReturn(shared));
    }

    /**
     * Closes the connection if there is one, PostgreSQL then releases its session advisory locks.
     */
    Mono<Void> destroy() {
        return Mono.defer(() -> {
            var shared = created.getAndSet(null);
            return shared == null ? Mono.empty() : Mono.from(shared.close()).onErrorComplete();
        });
    }
}
