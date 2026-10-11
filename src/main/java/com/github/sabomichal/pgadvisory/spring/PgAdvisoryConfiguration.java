package com.github.sabomichal.pgadvisory.spring;

import com.github.sabomichal.pgadvisory.AdvisoryLockListener;
import com.github.sabomichal.pgadvisory.LockRecordWriter;
import com.github.sabomichal.pgadvisory.PgAdvisory;
import com.github.sabomichal.pgadvisory.PgAdvisorySettings;
import com.github.sabomichal.pgadvisory.r2dbc.R2dbcPgAdvisory;
import io.r2dbc.spi.ConnectionFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Beans of {@link PgAdvisory}, imported by {@link EnablePgAdvisory} and by the auto-configuration.
 * <p>
 * The lock session needs a {@link PgAdvisoryLockConnectionFactory} bean: an unpooled factory of its own, never
 * the application's pool. The auto-configuration creates one; without Spring Boot, define it yourself.
 */
@Configuration(proxyBeanMethods = false)
public class PgAdvisoryConfiguration {

    @Bean
    public PgAdvisory<Object> pgAdvisory(ConnectionFactory connectionFactory,
                                         ObjectProvider<PgAdvisoryLockConnectionFactory> lockConnectionFactory,
                                         ObjectProvider<PgAdvisorySettings> settings,
                                         ObjectProvider<AdvisoryLockListener> listeners,
                                         ObjectProvider<LockRecordWriter> recordWriter) {
        var lockSessionFactory = lockConnectionFactory.getIfAvailable(() -> {
            throw new IllegalStateException("PgAdvisory needs a PgAdvisoryLockConnectionFactory bean: an unpooled "
                    + "ConnectionFactory for the lock session, e.g. ConnectionFactories.get(\"r2dbc:postgresql://...\")");
        }).connectionFactory();
        return R2dbcPgAdvisory.builder(lockSessionFactory)
                .workConnectionFactory(connectionFactory)
                .settings(settings.getIfAvailable(PgAdvisorySettings::defaults))
                .listeners(listeners.orderedStream().toList())
                .recordWriter(recordWriter.getIfAvailable())
                .build();
    }

    @Bean
    public PgAdvisoryLifecycle pgAdvisoryLifecycle(PgAdvisory<?> pgAdvisory, ObjectProvider<PgAdvisorySettings> settings) {
        return new PgAdvisoryLifecycle(pgAdvisory, settings.getIfAvailable(PgAdvisorySettings::defaults).lifecyclePhase());
    }

    @Bean
    public ApplicationEventPublishingListener pgAdvisoryEventPublishingListener(ApplicationEventPublisher publisher) {
        return new ApplicationEventPublishingListener(publisher);
    }
}
