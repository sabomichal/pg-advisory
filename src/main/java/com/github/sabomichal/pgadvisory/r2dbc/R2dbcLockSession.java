package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockSessionUnavailableException;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

final class R2dbcLockSession implements LockSession {

    private record Current(Connection connection, long incarnation) {
    }

    private final ConnectionFactory connectionFactory;
    private final LockStore store;
    private final String applicationName;
    private final SerialExecutor executor = new SerialExecutor();
    private final AtomicReference<Current> current = new AtomicReference<>();
    private final AtomicLong incarnations = new AtomicLong();

    R2dbcLockSession(ConnectionFactory connectionFactory, LockStore store, String applicationName) {
        this.connectionFactory = connectionFactory;
        this.store = store;
        // PostgreSQL truncates longer names
        this.applicationName = applicationName.length() > 63 ? applicationName.substring(0, 63) : applicationName;
    }

    @Override
    public Mono<Void> open() {
        return executor.submit(() -> Mono.from(connectionFactory.create())
                .flatMap(connection -> configure(connection)
                        .thenReturn(connection)
                        .onErrorResume(error -> closeQuietly(connection).then(Mono.error(error))))
                .doOnNext(connection -> replace(new Current(connection, incarnations.incrementAndGet())))
                .then());
    }

    @Override
    public <T> Mono<T> execute(Function<Connection, Mono<T>> work) {
        return executor.submit(() -> onCurrent(work));
    }

    @Override
    public <T> Mono<T> execute(Function<Connection, Mono<T>> work, Consumer<@Nullable T> onAbandoned) {
        return executor.submit(() -> onCurrent(work), onAbandoned);
    }

    private <T> Mono<T> onCurrent(Function<Connection, Mono<T>> work) {
        var session = current.get();
        return session == null ? Mono.error(new LockSessionUnavailableException()) : work.apply(session.connection());
    }

    @Override
    public <T> Mono<T> executeIfCurrent(long incarnation, Function<Connection, Mono<T>> work) {
        return executor.submit(() -> {
            var session = current.get();
            return session != null && session.incarnation() == incarnation ? work.apply(session.connection()) : Mono.empty();
        });
    }

    @Override
    public OptionalLong incarnation() {
        var session = current.get();
        return session == null ? OptionalLong.empty() : OptionalLong.of(session.incarnation());
    }

    @Override
    public Mono<Boolean> isAlive() {
        return execute(connection -> Mono.from(connection.validate(ValidationDepth.REMOTE)));
    }

    @Override
    public Mono<Boolean> isBroken(Throwable error) {
        if (error instanceof LockSessionUnavailableException || hasCause(error, R2dbcNonTransientResourceException.class)) {
            return Mono.just(true);
        }
        var session = current.get();
        if (session == null) {
            return Mono.just(true);
        }
        return Mono.from(session.connection().validate(ValidationDepth.LOCAL)).map(valid -> !valid);
    }

    @Override
    public void discard() {
        var session = current.getAndSet(null);
        if (session != null) {
            closeQuietly(session.connection()).subscribe();
        }
    }

    @Override
    public Mono<Void> close() {
        return executor.submit(() -> {
            var session = current.getAndSet(null);
            return session == null ? Mono.empty() : closeQuietly(session.connection());
        });
    }

    private Mono<Void> configure(Connection connection) {
        return Mono.from(connection.setAutoCommit(true)).then(store.setApplicationName(connection, applicationName));
    }

    private void replace(Current next) {
        var previous = current.getAndSet(next);
        if (previous != null) {
            closeQuietly(previous.connection()).subscribe();
        }
    }

    private static Mono<Void> closeQuietly(Connection connection) {
        return Mono.from(connection.close()).onErrorComplete();
    }

    private static boolean hasCause(Throwable error, Class<? extends Throwable> type) {
        for (var e = error; e != null; e = e.getCause()) {
            if (type.isInstance(e)) {
                return true;
            }
        }
        return false;
    }
}
