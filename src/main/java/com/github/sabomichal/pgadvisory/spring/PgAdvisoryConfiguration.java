package com.github.sabomichal.pgadvisory.spring;

import com.github.sabomichal.pgadvisory.ConnectionResetListener;
import com.github.sabomichal.pgadvisory.LockManager;
import com.github.sabomichal.pgadvisory.LockManagerSettings;
import com.github.sabomichal.pgadvisory.r2dbc.R2dbcLockManager;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The {@link LockManager} bean, imported by {@link EnablePgAdvisory} and by the auto-configuration.
 * <p>
 * The lock manager needs a {@link PgAdvisoryLockConnectionFactory} bean: an unpooled factory of its own, never
 * the application's pool. The auto-configuration creates one; without Spring Boot, define it yourself.
 * {@link ConnectionResetListener} beans are told when the lock connection was re-created and its locks are gone.
 */
@Configuration(proxyBeanMethods = false)
public class PgAdvisoryConfiguration {

    @Bean(destroyMethod = "close")
    public R2dbcLockManager lockManager(ObjectProvider<PgAdvisoryLockConnectionFactory> lockConnectionFactory,
                                        ObjectProvider<LockManagerSettings> settings,
                                        ObjectProvider<ConnectionResetListener> connectionResetListeners) {
        var connectionFactory = lockConnectionFactory.getIfAvailable(() -> {
            throw new IllegalStateException("LockManager needs a PgAdvisoryLockConnectionFactory bean: an unpooled "
                    + "ConnectionFactory for the lock connection, e.g. ConnectionFactories.get(\"r2dbc:postgresql://...\")");
        }).connectionFactory();
        var listeners = connectionResetListeners.orderedStream().toList();
        var lockManager = new R2dbcLockManager(connectionFactory, settings.getIfAvailable(LockManagerSettings::defaults),
                () -> listeners.forEach(ConnectionResetListener::onConnectionReset));
        lockManager.ensureTable().block();
        return lockManager;
    }
}
