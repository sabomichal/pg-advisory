package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.PgAdvisory;
import com.github.sabomichal.pgadvisory.PgAdvisorySettings;
import com.github.sabomichal.pgadvisory.micrometer.MicrometerAdvisoryLockListener;
import com.github.sabomichal.pgadvisory.spring.FencingR2dbcTransactionManager;
import com.github.sabomichal.pgadvisory.spring.PgAdvisoryConfiguration;
import com.github.sabomichal.pgadvisory.spring.PgAdvisoryLockConnectionFactory;
import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.health.contributor.ReactiveHealthIndicator;
import org.springframework.boot.r2dbc.autoconfigure.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcConnectionDetails;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcTransactionManagerAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.connection.R2dbcTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;

/** Active with {@code spring.pg-advisory.enabled=true}. */
@AutoConfiguration(
        after = R2dbcAutoConfiguration.class,
        before = R2dbcTransactionManagerAutoConfiguration.class,
        afterName = "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration")
@ConditionalOnClass({ConnectionFactory.class, R2dbcTransactionManager.class})
@ConditionalOnBooleanProperty("spring.pg-advisory.enabled")
@ConditionalOnBean(ConnectionFactory.class)
@EnableConfigurationProperties(PgAdvisoryProperties.class)
public class PgAdvisoryAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public PgAdvisorySettings pgAdvisorySettings(PgAdvisoryProperties properties) {
        return properties.toSettings();
    }

    /**
     * The lock session always gets a connection factory of its own, never the application's pool: from
     * {@code spring.pg-advisory.r2dbc.*} when set, otherwise from the application's connection details. Either
     * way the application's {@link ConnectionFactoryOptionsBuilderCustomizer}s apply, like they do to the
     * application's factory: the lock session must see the lock table in the same schema as the work connections.
     */
    @Bean
    @ConditionalOnMissingBean
    public PgAdvisoryLockConnectionFactory pgAdvisoryLockConnectionFactory(PgAdvisoryProperties properties,
                                                                           ObjectProvider<R2dbcConnectionDetails> connectionDetails,
                                                                           ObjectProvider<ConnectionFactoryOptionsBuilderCustomizer> customizers) {
        var r2dbc = properties.r2dbc();
        ConnectionFactoryOptions options;
        if (r2dbc.url() != null) {
            var builder = ConnectionFactoryOptions.parse(r2dbc.url()).mutate();
            if (r2dbc.username() != null) {
                builder.option(ConnectionFactoryOptions.USER, r2dbc.username());
            }
            if (r2dbc.password() != null) {
                builder.option(ConnectionFactoryOptions.PASSWORD, r2dbc.password());
            }
            options = builder.build();
        } else {
            var details = connectionDetails.getIfAvailable();
            if (details == null) {
                throw new IllegalStateException("Cannot create the connection factory of the PgAdvisory lock session: "
                        + "set spring.pg-advisory.r2dbc.url or define a PgAdvisoryLockConnectionFactory bean");
            }
            options = details.getConnectionFactoryOptions();
        }
        var customized = options.mutate();
        customizers.orderedStream().forEach(customizer -> customizer.customize(customized));
        return new PgAdvisoryLockConnectionFactory(ConnectionFactories.get(unpooled(customized.build())));
    }

    /** {@code r2dbc:pool:postgresql://...} becomes {@code r2dbc:postgresql://...}. */
    static ConnectionFactoryOptions unpooled(ConnectionFactoryOptions options) {
        if (!"pool".equals(options.getValue(ConnectionFactoryOptions.DRIVER))) {
            return options;
        }
        var protocol = String.valueOf(options.getRequiredValue(ConnectionFactoryOptions.PROTOCOL)).split(":", 2);
        return ConnectionFactoryOptions.builder()
                .from(options)
                .option(ConnectionFactoryOptions.DRIVER, protocol[0])
                .option(ConnectionFactoryOptions.PROTOCOL, protocol.length == 2 ? protocol[1] : "")
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(ReactiveTransactionManager.class)
    @ConditionalOnSingleCandidate(ConnectionFactory.class)
    @ConditionalOnBooleanProperty(name = "spring.pg-advisory.fencing.enabled", matchIfMissing = true)
    public FencingR2dbcTransactionManager transactionManager(ConnectionFactory connectionFactory) {
        return new FencingR2dbcTransactionManager(connectionFactory);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnMissingBean(PgAdvisory.class)
    @Import(PgAdvisoryConfiguration.class)
    static class PgAdvisoryCoreConfiguration {
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class PgAdvisoryMetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean
        MicrometerAdvisoryLockListener pgAdvisoryMetricsListener(MeterRegistry registry) {
            return new MicrometerAdvisoryLockListener(registry);
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(ReactiveHealthIndicator.class)
    static class PgAdvisoryHealthConfiguration {

        @Bean
        @ConditionalOnMissingBean(name = "pgAdvisoryHealthIndicator")
        PgAdvisoryHealthIndicator pgAdvisoryHealthIndicator(PgAdvisory<?> pgAdvisory) {
            return new PgAdvisoryHealthIndicator(pgAdvisory);
        }
    }
}
