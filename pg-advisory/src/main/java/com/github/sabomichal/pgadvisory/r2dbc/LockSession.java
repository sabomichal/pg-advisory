package com.github.sabomichal.pgadvisory.r2dbc;

import com.github.sabomichal.pgadvisory.LockManagerSettings;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.ValidationDepth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * One lock connection with its queue and heartbeat. A broken one is reset: it ends together with the operations still
 * queued on it, and the lock manager replaces it with a new session.
 */
final class LockSession {

    private static final Logger log = LoggerFactory.getLogger(LockSession.class);

    private final SingleConnectionFactory connectionFactory;
    private final LockManagerSettings settings;
    private final Runnable onReset;
    private final SerialExecutor executor = new SerialExecutor();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final Disposable heartbeat;

    /**
     * @param onReset called once when the session is found broken, after its connection was closed
     */
    LockSession(ConnectionFactory lockConnectionFactory, AdvisoryLockDao dao, LockManagerSettings settings, Runnable onReset) {
        this.connectionFactory = new SingleConnectionFactory(
                lockConnectionFactory,
                connection -> dao.setApplicationName(connection, settings.applicationName()));
        this.settings = settings;
        this.onReset = onReset;
        this.heartbeat = startHeartbeat();
    }

    /**
     * Runs {@code task} after the tasks submitted before it, regardless of caller cancellation. The task gets the lock
     * connection, which fails with {@link SessionFinishedException} once the session ended, so that no task reaches a connection
     * after it. A task submitted after the end runs at once, without waiting in the queue. Errors must be handled within
     * the task when they matter also to a caller that cancelled.
     */
    <T> Mono<T> execute(Function<Mono<Connection>, Mono<T>> task) {
        return execute(task, _ -> {
        });
    }

    /**
     * Like {@link #execute(Function)}, passes a value the caller cancelled before receiving to {@code onAbandoned}.
     */
    <T> Mono<T> execute(Function<Mono<Connection>, Mono<T>> task, Consumer<T> onAbandoned) {
        return Mono.defer(() -> finished.get()
                ? task.apply(Mono.error(new SessionFinishedException()))
                : executor.submit(() -> task.apply(createConnection()), onAbandoned));
    }

    /**
     * Validates the lock connection and resets the session when it is broken. Run by the heartbeat and after a
     * non-transient error.
     */
    Mono<Void> checkAndResetConnection() {
        // the reset happens within the task, so that the tasks queued behind it find the session ended
        return execute(lockConnection -> lockConnection
                // an unanswered validation counts as broken, it would hold up the queue of the lock connection
                .flatMap(connection -> Mono.from(connection.validate(ValidationDepth.REMOTE))
                        .timeout(settings.heartbeatTimeout(), Mono.just(false)))
                .flatMap(valid -> valid ? Mono.<Void>empty() : Mono.error(new ConnectionValidationException()))
                .doOnError(R2dbcNonTransientResourceException.class, _ -> reset())
                .doOnError(ConnectionValidationException.class, _ -> reset())
                .onErrorResume(R2dbcNonTransientResourceException.class, _ -> Mono.empty())
                .onErrorResume(ConnectionValidationException.class, _ -> Mono.empty())
                .onErrorResume(SessionFinishedException.class, _ -> Mono.empty()));
    }

    /**
     * Ends the session once: stops the heartbeat and closes the connection, which drops all its locks.
     */
    void reset() {
        if (finished.compareAndSet(false, true)) {
            heartbeat.dispose();
            connectionFactory.destroy().subscribe();
            onReset.run();
        }
    }

    /**
     * Ends the session after the queued tasks, but waits for them only up to the shutdown lock timeout, then closes the
     * connection anyway. Not a reset, {@code onReset} is not called.
     */
    Mono<Void> close() {
        return Mono.defer(() -> {
            heartbeat.dispose();
            return executor.submit(this::destroy)
                    .timeout(settings.shutdownLockTimeout(), Mono.defer(() -> {
                        log.warn("[{}] Operations on the lock connection did not finish within [{}], closing it anyway",
                                settings.applicationName(), settings.shutdownLockTimeout());
                        return destroy();
                    }));
        });
    }

    private Mono<Connection> createConnection() {
        return Mono.defer(() -> finished.get() ? Mono.error(new SessionFinishedException()) : connectionFactory.create());
    }

    private Mono<Void> destroy() {
        finished.set(true);
        return connectionFactory.destroy();
    }

    private Disposable startHeartbeat() {
        return checkAndResetConnection()
                .doOnError(error -> log.warn("[{}] Heartbeat of the lock connection failed", settings.applicationName(), error))
                .onErrorComplete()
                .delaySubscription(settings.heartbeatInterval())
                .repeat()
                .subscribe();
    }

    /**
     * The session ended before the task got the lock connection, after a reset or close.
     */
    static final class SessionFinishedException extends RuntimeException {

        SessionFinishedException() {
            super("The lock session ended before the operation ran");
        }
    }

    /**
     * The lock connection failed its validation.
     */
    private static class ConnectionValidationException extends RuntimeException {
    }
}
