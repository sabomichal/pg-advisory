package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.PgAdvisory;
import com.github.sabomichal.pgadvisory.PgAdvisory.ConnectionState;
import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome;
import com.github.sabomichal.pgadvisory.PgAdvisory.LostLockPolicy;
import com.github.sabomichal.pgadvisory.PgAdvisorySettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import com.github.sabomichal.pgadvisory.spring.EnablePgAdvisory;
import com.github.sabomichal.pgadvisory.spring.FencingR2dbcTransactionManager;
import com.github.sabomichal.pgadvisory.spring.PgAdvisoryEvents;
import com.github.sabomichal.pgadvisory.spring.PgAdvisoryLifecycle;
import com.github.sabomichal.pgadvisory.spring.PgAdvisoryLockConnectionFactory;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.r2dbc.autoconfigure.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcConnectionDetails;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcTransactionManagerAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.r2dbc.connection.DelegatingConnectionFactory;
import org.springframework.transaction.ReactiveTransactionManager;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PgAdvisoryAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(PgAdvisoryAutoConfiguration.class, R2dbcTransactionManagerAutoConfiguration.class))
            .withUserConfiguration(DatabaseConfiguration.class, EventCollector.class);

    @Test
    void nothingIsConfiguredWithoutPgAdvisoryEnabled() {
        runner.run(context -> {
            assertThat(context).doesNotHaveBean(PgAdvisory.class);
            assertThat(context.getBean(ReactiveTransactionManager.class)).isNotInstanceOf(FencingR2dbcTransactionManager.class);
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void startsTheLockSessionAndFencesTransactions() {
        runner.withPropertyValues(
                        "spring.pg-advisory.enabled=true",
                        "spring.pg-advisory.owner=spring",
                        "spring.pg-advisory.table-name=spring_advisory_lock",
                        "spring.pg-advisory.schema.auto-create=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    PgAdvisory<Object> pgAdvisory = context.getBean(PgAdvisory.class);
                    assertThat(pgAdvisory.state()).isEqualTo(ConnectionState.CONNECTED);
                    assertThat(pgAdvisory.owner()).isEqualTo("spring");
                    assertThat(context.getBean(ReactiveTransactionManager.class)).isInstanceOf(FencingR2dbcTransactionManager.class);
                    assertThat(context).hasSingleBean(PgAdvisoryLifecycle.class);
                    assertThat(context).hasBean("pgAdvisoryHealthIndicator");

                    var outcome = await(pgAdvisory.withLock(UUID.randomUUID(), handle -> Mono.just("ok")));

                    assertThat(outcome).isEqualTo(new LockOutcome.Completed<>("ok", 1L));
                    assertThat(context.getBean(EventCollector.class).events)
                            .hasExactlyElementsOfTypes(PgAdvisoryEvents.LockAcquired.class, PgAdvisoryEvents.LockReleased.class);
                });
    }

    @Test
    void propertyDefaultsMatchSettingsDefaults() {
        runner.withPropertyValues("spring.pg-advisory.enabled=true", "spring.pg-advisory.schema.auto-create=true")
                .run(context -> {
                    var settings = context.getBean(PgAdvisorySettings.class);
                    var expected = PgAdvisorySettings.defaults().toBuilder().autoCreateSchema(true).build();
                    assertThat(settings).isEqualTo(expected);
                });
    }

    @Test
    void fencingTransactionManagerCanBeDisabled() {
        runner.withPropertyValues("spring.pg-advisory.enabled=true", "spring.pg-advisory.schema.auto-create=true", "spring.pg-advisory.fencing.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(PgAdvisory.class);
                    assertThat(context.getBean(ReactiveTransactionManager.class)).isNotInstanceOf(FencingR2dbcTransactionManager.class);
                });
    }

    @Test
    void lostLockPolicyIsBound() {
        runner.withPropertyValues("spring.pg-advisory.enabled=true", "spring.pg-advisory.schema.auto-create=true",
                        "spring.pg-advisory.lost-lock-policy=NOTIFY")
                .run(context -> assertThat(context.getBean(PgAdvisorySettings.class).lostLockPolicy()).isEqualTo(LostLockPolicy.NOTIFY));
    }

    @Test
    @SuppressWarnings("unchecked")
    void lockSessionNeverUsesTheApplicationPool() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgAdvisoryAutoConfiguration.class, R2dbcTransactionManagerAutoConfiguration.class))
                .withUserConfiguration(SingleConnectionPoolConfiguration.class)
                .withPropertyValues("spring.pg-advisory.enabled=true", "spring.pg-advisory.schema.auto-create=true")
                .run(context -> {
                    PgAdvisory<Object> pgAdvisory = context.getBean(PgAdvisory.class);

                    // the pool's only connection stays free for the takeover, the lock session has its own
                    var outcome = pgAdvisory.withLock(UUID.randomUUID(), handle -> Mono.just("ok")).block(Duration.ofSeconds(5));

                    assertThat(outcome).isInstanceOf(LockOutcome.Completed.class);
                });
    }

    @Test
    @SuppressWarnings("unchecked")
    void lockSessionGetsTheOptionsCustomizersOfTheApplication() {
        var schema = "customized_" + UUID.randomUUID().toString().replace("-", "");
        await(TestPostgres.withConnection(connection -> execute(connection, "CREATE SCHEMA " + schema)));

        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(R2dbcAutoConfiguration.class, PgAdvisoryAutoConfiguration.class,
                        R2dbcTransactionManagerAutoConfiguration.class))
                .withBean(ConnectionFactoryOptionsBuilderCustomizer.class,
                        () -> options -> options.option(Option.<String>valueOf("schema"), schema))
                .withPropertyValues(
                        "spring.r2dbc.url=" + TestPostgres.url("r2dbc:pool:postgresql"),
                        "spring.r2dbc.username=" + TestPostgres.username(),
                        "spring.r2dbc.password=" + TestPostgres.password(),
                        "spring.pg-advisory.enabled=true",
                        "spring.pg-advisory.schema.auto-create=true")
                .run(context -> {
                    PgAdvisory<Object> pgAdvisory = context.getBean(PgAdvisory.class);

                    // the takeover runs on the application's connection, so it must see the same lock table
                    var outcome = await(pgAdvisory.withLock(UUID.randomUUID(), handle -> Mono.just("ok")));

                    var lockTables = TestPostgres.withConnection(connection -> execute(connection,
                            "SELECT 1 WHERE to_regclass($1) IS NOT NULL", schema + ".advisory_lock"));
                    assertThat(outcome).isInstanceOf(LockOutcome.Completed.class);
                    assertThat(await(lockTables)).isEqualTo(1L);
                });
    }

    @Test
    void failsWithoutConnectionDetailsOfTheLockSession() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(PgAdvisoryAutoConfiguration.class, R2dbcTransactionManagerAutoConfiguration.class))
                .withBean(ConnectionFactory.class, TestPostgres::pool)
                .withPropertyValues("spring.pg-advisory.enabled=true")
                .run(context -> assertThat(context).getFailure().rootCause().hasMessageContaining("spring.pg-advisory.r2dbc.url"));
    }

    @Test
    void enablePgAdvisoryRequiresALockConnectionFactory() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(EnableWithoutLockFactoryConfiguration.class))
                .isInstanceOf(BeanCreationException.class)
                .hasRootCauseMessage("PgAdvisory needs a PgAdvisoryLockConnectionFactory bean: an unpooled ConnectionFactory for the "
                        + "lock session, e.g. ConnectionFactories.get(\"r2dbc:postgresql://...\")");
    }

    @Test
    void enablePgAdvisoryWorksWithoutSpringBoot() {
        PgAdvisory<?> pgAdvisory;
        try (var context = new AnnotationConfigApplicationContext(EnableConfiguration.class)) {
            pgAdvisory = context.getBean(PgAdvisory.class);
            assertThat(pgAdvisory.state()).isEqualTo(ConnectionState.CONNECTED);
        }
        assertThat(pgAdvisory.state()).isEqualTo(ConnectionState.STOPPED);
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

    /** A pool of a single connection, wrapped twice. */
    @Configuration(proxyBeanMethods = false)
    static class SingleConnectionPoolConfiguration {

        @Bean
        ConnectionFactory connectionFactory() {
            var pool = new ConnectionPool(ConnectionPoolConfiguration.builder(TestPostgres.connectionFactory()).initialSize(1).maxSize(1).build());
            return new DelegatingConnectionFactory(new DelegatingConnectionFactory(pool));
        }

        @Bean
        R2dbcConnectionDetails r2dbcConnectionDetails() {
            return () -> TestPostgres.options("r2dbc:pool:postgresql");
        }
    }

    static class EventCollector {

        final List<Object> events = new CopyOnWriteArrayList<>();

        @EventListener
        void on(PgAdvisoryEvents.LockAcquired event) {
            events.add(event);
        }

        @EventListener
        void on(PgAdvisoryEvents.LockReleased event) {
            events.add(event);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnablePgAdvisory
    static class EnableConfiguration {

        @Bean
        ConnectionFactory connectionFactory() {
            return TestPostgres.pool();
        }

        @Bean
        PgAdvisoryLockConnectionFactory pgAdvisoryLockConnectionFactory() {
            return new PgAdvisoryLockConnectionFactory(TestPostgres.connectionFactory());
        }

        @Bean
        PgAdvisorySettings pgAdvisorySettings() {
            return PgAdvisorySettings.builder().owner("enable").autoCreateSchema(true).build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnablePgAdvisory
    static class EnableWithoutLockFactoryConfiguration {

        @Bean
        ConnectionFactory connectionFactory() {
            return TestPostgres.pool();
        }
    }
}
