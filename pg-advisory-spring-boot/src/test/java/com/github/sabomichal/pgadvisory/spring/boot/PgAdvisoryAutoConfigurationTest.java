package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManager.LockHandle;
import com.github.sabomichal.pgadvisory.LockManager.LockOutcome;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import com.github.sabomichal.pgadvisory.micrometer.MicrometerLockManagerListener;
import com.github.sabomichal.pgadvisory.r2dbc.R2dbcLockManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.r2dbc.autoconfigure.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcConnectionDetails;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.execute;
import static org.assertj.core.api.Assertions.assertThat;

class PgAdvisoryAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PgAdvisoryAutoConfiguration.class))
            .withUserConfiguration(DatabaseConfiguration.class);

    @Test
    void isRegisteredAsAutoConfiguration() {
        var candidates = ImportCandidates.load(AutoConfiguration.class, getClass().getClassLoader()).getCandidates();

        assertThat(candidates).contains(PgAdvisoryAutoConfiguration.class.getName());
    }

    @Test
    void createsTheLockManagerWithoutBeingEnabled() {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            var key = UUID.randomUUID().toString();

            assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(new LockOutcome.Completed<>(key));
        });
    }

    @Test
    void canBeSwitchedOff() {
        runner.withPropertyValues("pg-advisory.enabled=false")
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(LockManager.class));
    }

    @Test
    void backsOffWhenThereIsNothingToConnectTheLockManagerWith() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgAdvisoryAutoConfiguration.class))
                .run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(LockManager.class));
    }

    @Test
    void backsOffForTheApplicationsOwnLockManager() {
        runner.withBean("ownLockManager", LockManager.class,
                        () -> new R2dbcLockManager(TestPostgres.connectionFactory(), LockManagerSettings.defaults()))
                .run(context -> assertThat(context).hasNotFailed().hasSingleBean(LockManager.class).hasBean("ownLockManager"));
    }

    @Test
    void usesTheApplicationsOwnLockConnectionFactory() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgAdvisoryAutoConfiguration.class))
                .withBean(PgAdvisoryLockConnectionFactory.class, () -> new PgAdvisoryLockConnectionFactory(TestPostgres.connectionFactory()))
                .run(context -> {
                    var key = UUID.randomUUID().toString();

                    assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(new LockOutcome.Completed<>(key));
                });
    }

    @Test
    void propertyDefaultsMatchSettingsDefaults() {
        runner.run(context -> assertThat(context.getBean(LockManagerSettings.class)).isEqualTo(LockManagerSettings.defaults()));
    }

    @Test
    void lockConnectionGetsTheOptionsCustomizersOfTheApplication() {
        var schema = "customized_" + UUID.randomUUID().toString().replace("-", "");
        await(TestPostgres.withConnection(connection -> execute(connection, "CREATE SCHEMA " + schema)));

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(R2dbcAutoConfiguration.class, PgAdvisoryAutoConfiguration.class))
                .withBean(ConnectionFactoryOptionsBuilderCustomizer.class,
                        () -> options -> options.option(Option.<String>valueOf("schema"), schema))
                .withPropertyValues(TestPostgres.r2dbcProperties())
                // the lock table does not exist in the new schema yet
                .withPropertyValues("pg-advisory.schema.auto-create=true")
                .run(context -> {
                    var key = UUID.randomUUID().toString();

                    assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(new LockOutcome.Completed<>(key));
                    var lockTables = TestPostgres.withConnection(connection -> execute(connection,
                            "SELECT 1 WHERE to_regclass($1) IS NOT NULL", schema + ".advisory_lock"));
                    assertThat(await(lockTables)).isEqualTo(1L);
                });
    }

    @Test
    void passesTheApplicationsListeners() {
        var locked = new CopyOnWriteArrayList<String>();
        runner.withBean(LockManagerListener.class, () -> new LockManagerListener() {
                    @Override
                    public void onLocked(LockHandle handle, Duration lockDuration) {
                        locked.add(handle.key());
                    }
                })
                .run(context -> {
                    var key = UUID.randomUUID().toString();

                    lockAndUnlock(context.getBean(LockManager.class), key);

                    assertThat(locked).containsExactly(key);
                });
    }

    @Test
    void publishesEvents() {
        runner.withUserConfiguration(EventCollector.class).run(context -> {
            lockAndUnlock(context.getBean(LockManager.class), UUID.randomUUID().toString());

            assertThat(context.getBean(EventCollector.class).events)
                    .hasExactlyElementsOfTypes(PgAdvisoryEvents.Locked.class, PgAdvisoryEvents.Released.class);
        });
    }

    @Test
    void recordsMetricsWithAMeterRegistry() {
        runner.withBean(MeterRegistry.class, SimpleMeterRegistry::new).run(context -> {
            lockAndUnlock(context.getBean(LockManager.class), UUID.randomUUID().toString());

            var registry = context.getBean(MeterRegistry.class);
            assertThat(registry.get("pg.advisory.lock").tag("outcome", "locked").timer().count()).isEqualTo(1);
            assertThat(registry.get("pg.advisory.hold").timer().count()).isEqualTo(1);
        });
    }

    @Test
    void noMetricsWithoutAMeterRegistry() {
        runner.run(context -> assertThat(context).hasNotFailed().doesNotHaveBean(MicrometerLockManagerListener.class));
    }

    @Test
    void worksWithoutMicrometer() {
        runner.withClassLoader(new FilteredClassLoader("io.micrometer")).run(context -> {
            var key = UUID.randomUUID().toString();

            assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(new LockOutcome.Completed<>(key));
        });
    }

    @Configuration(proxyBeanMethods = false)
    static class DatabaseConfiguration {

        @Bean
        ConnectionFactory connectionFactory() {
            return TestPostgres.pool();
        }

        /** Like Spring Boot's details of {@code spring.r2dbc.url=r2dbc:pool:postgresql://...}. */
        @Bean
        R2dbcConnectionDetails r2dbcConnectionDetails() {
            return () -> TestPostgres.options("r2dbc:pool:postgresql");
        }
    }

    static class EventCollector {

        final List<PgAdvisoryEvents> events = new CopyOnWriteArrayList<>();

        @EventListener
        void on(PgAdvisoryEvents event) {
            events.add(event);
        }
    }

    /** Locks {@code key}, returns it from the locked work and unlocks. */
    private static LockOutcome<String> lockAndUnlock(LockManager lockManager, String key) {
        return await(lockManager.tryWithLock(key, handle -> Mono.just(handle.key())));
    }
}
