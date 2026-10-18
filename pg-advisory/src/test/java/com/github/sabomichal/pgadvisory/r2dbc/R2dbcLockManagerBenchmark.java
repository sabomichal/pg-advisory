package com.github.sabomichal.pgadvisory.r2dbc;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.CommandLineOptions;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * JMH benchmarks of one lock manager on the {@link TestPostgres} container of its fork, run with {@link #main}.
 * Latency percentiles of one lock/unlock cycle by default, {@link #throughput} overrides the mode.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 2, time = 5)
@Measurement(iterations = 3, time = 5)
@Fork(1)
public class R2dbcLockManagerBenchmark {

    private static final int KEY_COUNT = 1000;

    /** Fixed keys, so that their rows exist after the first round, like keys that repeat in real use. */
    private static final List<String> KEYS = IntStream.range(0, KEY_COUNT).mapToObj(i -> "benchmark-" + i).toList();

    private final AtomicInteger next = new AtomicInteger();
    private R2dbcLockManager lockManager;

    /** {@code args} are JMH command line options, e.g. {@code latency -wi 1 -i 2}. Without a pattern, all run. */
    static void main(String[] args) throws Exception {
        new Runner(new OptionsBuilder().parent(new CommandLineOptions(args)).build()).run();
    }

    @Setup
    public void setUp() {
        // logging of every statement would distort the measurement
        ((Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME)).setLevel(Level.WARN);
        lockManager = new R2dbcLockManager(TestPostgres.connectionFactory(),
                LockManagerSettings.builder().applicationName("pg-advisory-benchmark").build());
    }

    @TearDown
    public void tearDown() {
        lockManager.close();
    }

    /** Lock/unlock cycles per second, every key once, {@link ThroughputParams#concurrency} keys at a time. */
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    @OperationsPerInvocation(KEY_COUNT)
    public List<LockOutcome<String>> throughput(ThroughputParams params) {
        return Flux.fromIterable(KEYS)
                .flatMap(this::lockAndUnlock, params.concurrency)
                .collectList()
                .block();
    }

    @Benchmark
    @Threads(1)
    public LockOutcome<String> latency1() {
        return lockAndUnlockNext();
    }

    @Benchmark
    @Threads(16)
    public LockOutcome<String> latency16() {
        return lockAndUnlockNext();
    }

    @Benchmark
    @Threads(256)
    public LockOutcome<String> latency256() {
        return lockAndUnlockNext();
    }

    /** The keys in turn, so that concurrent callers do not collide on a key. */
    private LockOutcome<String> lockAndUnlockNext() {
        return lockAndUnlock(KEYS.get(Math.floorMod(next.getAndIncrement(), KEY_COUNT))).block();
    }

    /** Locks {@code key} and unlocks it right away. */
    private Mono<LockOutcome<String>> lockAndUnlock(String key) {
        return lockManager.tryWithLock(key, handle -> Mono.just(handle.key()));
    }

    /** In its own state, so that the latency benchmarks do not run once per value. */
    @State(Scope.Benchmark)
    public static class ThroughputParams {

        /** Lock attempts running at once. */
        @Param({"1", "16", "256"})
        public int concurrency;
    }
}
