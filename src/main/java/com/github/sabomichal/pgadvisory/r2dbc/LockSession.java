package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockSessionUnavailableException;
import com.github.sabomichal.pgadvisory.PgAdvisory;
import io.r2dbc.spi.Connection;
import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;

import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The dedicated connection that holds all session advisory locks of one {@link PgAdvisory}.
 * Work on it is serialized. Every (re)connect starts a new <em>incarnation</em>: the locks of an older
 * incarnation were released by PostgreSQL together with its connection.
 */
interface LockSession {

    Mono<Void> open();

    /** Runs {@code work} on the connection; fails with {@link LockSessionUnavailableException} when disconnected. */
    <T> Mono<T> execute(Function<Connection, Mono<T>> work);

    /**
     * Like {@link #execute(Function)}; when the caller cancelled before receiving the result, {@code onAbandoned}
     * gets it instead: the value, or {@code null} when the work failed or completed empty.
     */
    <T> Mono<T> execute(Function<Connection, Mono<T>> work, Consumer<@Nullable T> onAbandoned);

    /** Runs {@code work} only if the session still is {@code incarnation}, otherwise completes empty. */
    <T> Mono<T> executeIfCurrent(long incarnation, Function<Connection, Mono<T>> work);

    /** The current incarnation, empty when disconnected. */
    OptionalLong incarnation();

    /** Round trip to the database. */
    Mono<Boolean> isAlive();

    /** Whether {@code error} means that the connection is gone. */
    Mono<Boolean> isBroken(Throwable error);

    /** Drops the connection without waiting for running statements; used when it is broken anyway. */
    void discard();

    Mono<Void> close();
}
