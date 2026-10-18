package com.github.sabomichal.pgadvisory.r2dbc;

import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Runs tasks one at a time in the order of submission, without polling. An R2DBC connection serves one operation at a
 * time, so every use of the lock connection goes through here.
 */
final class SerialExecutor {

    private final Sinks.Many<Mono<Void>> queue = Sinks.many().unicast().onBackpressureBuffer();

    SerialExecutor() {
        queue.asFlux().concatMap(task -> task).subscribe();
    }

    /**
     * Runs {@code task} after the tasks submitted before, and to its end even when the caller cancels: stopping it
     * midway would leave the lock connection in an unknown state.
     */
    <T> Mono<T> submit(Supplier<Mono<T>> task) {
        return Mono.create(sink -> {
            Mono<Void> run = Mono.defer(task)
                    .doOnSuccess(sink::success)
                    .doOnError(sink::error)
                    .onErrorComplete()
                    .then();
            queue.emitNext(run, Sinks.EmitFailureHandler.busyLooping(Duration.ofSeconds(1)));
        });
    }
}
