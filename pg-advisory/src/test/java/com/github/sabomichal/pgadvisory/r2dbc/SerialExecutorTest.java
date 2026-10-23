package com.github.sabomichal.pgadvisory.r2dbc;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SerialExecutorTest {

    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private final SerialExecutor executor = new SerialExecutor();

    @Test
    void runsTasksOneAtATimeInOrder() {
        var running = new AtomicInteger();
        var maxRunning = new AtomicInteger();
        var order = new CopyOnWriteArrayList<Integer>();

        Flux.range(0, 100)
                .flatMap(i -> executor.submit(() -> Mono.fromRunnable(() -> maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max))
                        .then(Mono.delay(Duration.ofMillis(1)))
                        .doOnSuccess(_ -> {
                            order.add(i);
                            running.decrementAndGet();
                        })), 100)
                .blockLast(TIMEOUT);

        assertThat(maxRunning).hasValue(1);
        assertThat(order).containsExactlyElementsOf(IntStream.range(0, 100).boxed().toList());
    }

    @Test
    void runsTheTaskToItsEndWhenTheCallerCancels() {
        var started = new CompletableFuture<Void>();
        var finished = new CompletableFuture<Void>();

        var cancelled = executor.submit(() -> Mono.fromRunnable(() -> started.complete(null))
                        .then(Mono.delay(Duration.ofMillis(100)))
                        .doOnSuccess(_ -> finished.complete(null)))
                .subscribe();
        started.join();
        cancelled.dispose();

        assertThat(executor.submit(() -> Mono.just("next")).block(TIMEOUT)).isEqualTo("next");
        assertThat(finished).isDone();
    }

    @Test
    void handsTheValueToOnAbandonedWhenTheCallerCancelled() {
        var release = new CompletableFuture<String>();
        var abandoned = new CopyOnWriteArrayList<String>();

        executor.submit(() -> Mono.fromFuture(release), abandoned::add).subscribe().dispose();
        release.complete("value");

        executor.submit(() -> Mono.just("next")).block(TIMEOUT);
        assertThat(abandoned).containsExactly("value");
    }

    @Test
    void deliveredValueOrFailureIsNotAbandoned() {
        var abandoned = new CopyOnWriteArrayList<String>();
        var failing = new CompletableFuture<String>();

        assertThat(executor.submit(() -> Mono.just("value"), abandoned::add).block(TIMEOUT)).isEqualTo("value");
        executor.submit(() -> Mono.fromFuture(failing), abandoned::add).subscribe(null, _ -> {}).dispose();
        failing.completeExceptionally(new IllegalStateException("task failed"));

        executor.submit(() -> Mono.just("next")).block(TIMEOUT);
        assertThat(abandoned).isEmpty();
    }

    @Test
    void failedTaskDoesNotStopTheQueue() {
        var failure = new IllegalStateException("task failed");

        assertThatThrownBy(() -> executor.submit(() -> Mono.error(failure)).block(TIMEOUT)).isSameAs(failure);
        assertThat(executor.submit(() -> Mono.just("next")).block(TIMEOUT)).isEqualTo("next");
    }
}
