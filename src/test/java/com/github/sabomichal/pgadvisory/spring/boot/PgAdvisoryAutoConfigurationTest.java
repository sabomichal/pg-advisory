package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.TestPostgres;
import com.github.sabomichal.pgadvisory.r2dbc.R2dbcLockManager;
import com.github.sabomichal.pgadvisory.spring.EnablePgAdvisory;
import com.github.sabomichal.pgadvisory.spring.PgAdvisoryLockConnectionFactory;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Option;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.context.annotation.ImportCandidates;
import org.springframework.boot.r2dbc.autoconfigure.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcConnectionDetails;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;

import java.util.UUID;

import static com.github.sabomichal.pgadvisory.TestPostgres.await;
import static com.github.sabomichal.pgadvisory.TestPostgres.execute;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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

            assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(key);
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

                    assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(key);
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

                    assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(key);
                    var lockTables = TestPostgres.withConnection(connection -> execute(connection,
                            "SELECT 1 WHERE to_regclass($1) IS NOT NULL", schema + ".advisory_lock"));
                    assertThat(await(lockTables)).isEqualTo(1L);
                });
    }

    @Test
    void enablePgAdvisoryWorksWithoutSpringBoot() {
        try (var context = new AnnotationConfigApplicationContext(EnableConfiguration.class)) {
            var key = UUID.randomUUID().toString();

            assertThat(lockAndUnlock(context.getBean(LockManager.class), key)).isEqualTo(key);
        }
    }

    @Test
    void enablePgAdvisoryRequiresALockConnectionFactory() {
        assertThatThrownBy(() -> new AnnotationConfigApplicationContext(EnableWithoutLockFactoryConfiguration.class))
                .isInstanceOf(BeanCreationException.class)
                .hasRootCauseMessage("LockManager needs a PgAdvisoryLockConnectionFactory bean: an unpooled ConnectionFactory for the "
                        + "lock connection, e.g. ConnectionFactories.get(\"r2dbc:postgresql://...\")");
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

    @Configuration(proxyBeanMethods = false)
    @EnablePgAdvisory
    static class EnableConfiguration {

        @Bean
        PgAdvisoryLockConnectionFactory pgAdvisoryLockConnectionFactory() {
            return new PgAdvisoryLockConnectionFactory(TestPostgres.connectionFactory());
        }

        @Bean
        LockManagerSettings lockManagerSettings() {
            return LockManagerSettings.builder().applicationName("enable").build();
        }
    }

    @Configuration(proxyBeanMethods = false)
    @EnablePgAdvisory
    static class EnableWithoutLockFactoryConfiguration {
    }

    /** Locks {@code key}, returns it from the locked function and unlocks. */
    private static String lockAndUnlock(LockManager lockManager, String key) {
        return await(lockManager.tryWithLock(key, Mono::just));
    }
}
