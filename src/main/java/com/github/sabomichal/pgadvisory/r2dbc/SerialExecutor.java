package com.github.sabomichal.pgadvisory.r2dbc;

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Runs tasks one at a time, in submission order. An R2DBC connection cannot interleave statements of
 * different transactions, so every use of the lock session goes through here.
 * <p>
 * A task runs to completion even if its caller cancels: stopping in the middle of a transaction would
 * leave the lock session in an unknown state.
 */
final class SerialExecutor {

    private final Sinks.Many<Mono<Void>> queue = Sinks.many().unicast().onBackpressureBuffer();

    SerialExecutor() {
        queue.asFlux().concatMap(task -> task).subscribe();
    }

    <T> Mono<T> submit(Supplier<Mono<T>> task) {
        return submit(task, _ -> {
        });
    }

    /**
     * Like {@link #submit(Supplier)}; when the caller cancelled before receiving the result, {@code onAbandoned}
     * gets it instead, exactly once: the value, or {@code null} when the task failed or completed empty.
     * Lets the caller release what the task acquired for it.
     */
    <T> Mono<T> submit(Supplier<Mono<T>> task, Consumer<@Nullable T> onAbandoned) {
        return Mono.create(sink -> {
            var abandonment = new Abandonment<>(onAbandoned);
            // MonoSink calls it only when the cancel wins over the delivery of the value
            sink.onCancel(abandonment::cancelled);
            Mono<Void> run = Mono.defer(task)
                    .doOnSuccess(value -> {
                        abandonment.finished(value);
                        sink.success(value);
                    })
                    .doOnError(error -> {
                        abandonment.finished(null);
                        sink.error(error);
                    })
                    .onErrorComplete()
                    .then();
            queue.emitNext(run, Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
        });
    }

    private static final class Abandonment<T> {

        private static final Object PENDING = new Object();
        private static final Object CLAIMED = new Object();

        private final Consumer<@Nullable T> onAbandoned;
        private final AtomicReference<@Nullable Object> result = new AtomicReference<>(PENDING);
        private volatile boolean cancelled;

        Abandonment(Consumer<@Nullable T> onAbandoned) {
            this.onAbandoned = onAbandoned;
        }

        void cancelled() {
            cancelled = true;
            claim();
        }

        void finished(@Nullable T value) {
            result.set(value);
            if (cancelled) {
                claim();
            }
        }

        @SuppressWarnings("unchecked")
        private void claim() {
            var current = result.get();
            if (current != PENDING && current != CLAIMED && result.compareAndSet(current, CLAIMED)) {
                onAbandoned.accept((T) current);
            }
        }
    }
}
