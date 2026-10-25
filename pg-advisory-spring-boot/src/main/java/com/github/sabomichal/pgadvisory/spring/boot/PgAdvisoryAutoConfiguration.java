package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerListener;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.micrometer.MicrometerLockManagerListener;
import com.github.sabomichal.pgadvisory.r2dbc.R2dbcLockManager;
import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryOptions;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.r2dbc.autoconfigure.ConnectionFactoryOptionsBuilderCustomizer;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcAutoConfiguration;
import org.springframework.boot.r2dbc.autoconfigure.R2dbcConnectionDetails;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

/**
 * Creates the {@link LockManager} when there is something to connect it with: {@code pg-advisory.r2dbc.url}, the
 * application's R2DBC connection details, or a {@link PgAdvisoryLockConnectionFactory} bean. Backs off when the
 * application defines its own {@code LockManager} or sets {@code pg-advisory.enabled=false}.
 */
@AutoConfiguration(after = R2dbcAutoConfiguration.class,
        afterName = "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration")
@ConditionalOnClass({ConnectionFactory.class, R2dbcConnectionDetails.class})
@ConditionalOnBooleanProperty(name = "pg-advisory.enabled", matchIfMissing = true)
@ConditionalOnMissingBean(LockManager.class)
@Conditional(PgAdvisoryAutoConfiguration.LockConnectionAvailable.class)
@EnableConfigurationProperties(PgAdvisoryProperties.class)
public class PgAdvisoryAutoConfiguration {

    /** Verifies the lock table on start. Gets all {@link LockManagerListener} beans, in their order. */
    @Bean(destroyMethod = "close")
    public R2dbcLockManager lockManager(PgAdvisoryLockConnectionFactory lockConnectionFactory, LockManagerSettings settings,
                                        ObjectProvider<LockManagerListener> listeners) {
        R2dbcLockManager.ensureTable(lockConnectionFactory.connectionFactory(), settings).block();
        return new R2dbcLockManager(lockConnectionFactory.connectionFactory(), settings, listeners.orderedStream().toList());
    }

    /** From the {@code pg-advisory.*} properties. */
    @Bean
    @ConditionalOnMissingBean
    public LockManagerSettings lockManagerSettings(PgAdvisoryProperties properties) {
        return properties.toSettings();
    }

    /** Publishes the callbacks of the lock manager as {@link PgAdvisoryEvents}. */
    @Bean
    @ConditionalOnMissingBean
    public ApplicationEventPublishingListener pgAdvisoryEventPublishingListener(ApplicationEventPublisher publisher) {
        return new ApplicationEventPublishingListener(publisher);
    }

    /**
     * The lock manager always gets a connection factory of its own, never the application's pool: from
     * {@code pg-advisory.r2dbc.*} when set, otherwise from the application's connection details. Either way the
     * application's {@link ConnectionFactoryOptionsBuilderCustomizer}s apply, like they do to the application's
     * factory, so that the lock connection sees the same database and schema.
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
            options = connectionDetails.getObject().getConnectionFactoryOptions();
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

    /** Metrics when the application has a {@link MeterRegistry}, e.g. from Spring Boot Actuator. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(MeterRegistry.class)
    @ConditionalOnBean(MeterRegistry.class)
    static class PgAdvisoryMetricsConfiguration {

        @Bean
        @ConditionalOnMissingBean
        MicrometerLockManagerListener pgAdvisoryMetricsListener(MeterRegistry registry) {
            return new MicrometerLockManagerListener(registry);
        }
    }

    /** There is something to create the lock connection from. */
    static class LockConnectionAvailable extends AnyNestedCondition {

        LockConnectionAvailable() {
            super(ConfigurationPhase.REGISTER_BEAN);
        }

        @ConditionalOnProperty("pg-advisory.r2dbc.url")
        static class OwnUrl {
        }

        @ConditionalOnBean(R2dbcConnectionDetails.class)
        static class ApplicationConnectionDetails {
        }

        @ConditionalOnBean(PgAdvisoryLockConnectionFactory.class)
        static class OwnConnectionFactory {
        }
    }
}
