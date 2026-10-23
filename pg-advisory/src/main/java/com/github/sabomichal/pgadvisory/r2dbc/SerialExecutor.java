package com.github.sabomichal.pgadvisory.r2dbc;

import org.jspecify.annotations.Nullable;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Runs tasks one at a time in the order of submission, without polling. An R2DBC connection serves one operation at a
 * time, so every use of the lock connection goes through here.
 */
final class SerialExecutor {

    private static final Object CANCELLED = new Object();

    private final Sinks.Many<Mono<Void>> queue = Sinks.many().unicast().onBackpressureBuffer();

    SerialExecutor() {
        queue.asFlux().concatMap(task -> task).subscribe();
    }

    /**
     * Executes tasks serially, regardless of caller cancellation.
     */
    <T> Mono<T> submit(Supplier<Mono<T>> task) {
        return submit(task, _ -> {});
    }

    /**
     * Passes results not delivered due to cancellation to {@code onAbandoned}.
     */
    @SuppressWarnings("unchecked")
    <T> Mono<T> submit(Supplier<Mono<T>> task, Consumer<T> onAbandoned) {
        return Mono.create(sink -> {
            // null while running, then CANCELLED or the value, whichever comes first
            var cancelState = new AtomicReference<@Nullable Object>();
            // MonoSink calls it only when the cancel wins over the delivery of the value
            sink.onCancel(() -> {
                var value = cancelState.getAndSet(CANCELLED);
                if (value != null) {
                    onAbandoned.accept((T) value);
                }
            });
            Mono<Void> run = Mono.defer(task)
                    .doOnSuccess(value -> {
                        if (value != null && !cancelState.compareAndSet(null, value)) {
                            onAbandoned.accept(value);
                        }
                        sink.success(value);
                    })
                    .doOnError(sink::error)
                    .onErrorComplete()
                    .then();
            queue.emitNext(run, Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
        });
    }
}
