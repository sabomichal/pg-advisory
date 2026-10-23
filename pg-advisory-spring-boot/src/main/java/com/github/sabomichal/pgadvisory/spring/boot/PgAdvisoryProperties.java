package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.LockManagerSettings;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuration of pg-advisory. The defaults are the same as {@link LockManagerSettings#defaults()}.
 *
 * @param enabled             Whether to enable the auto-configuration.
 * @param tableName           Lock table, optionally schema qualified.
 * @param applicationName     application_name of the lock connection, visible in pg_stat_activity.
 * @param shutdownLockTimeout How long closing the lock manager waits for the queued operations, then it closes the
 *                            lock connection anyway.
 * @param heartbeatInterval   How often the lock connection is validated, a broken one is re-created.
 * @param heartbeatTimeout    How long a validation of the lock connection may take, then it counts as broken.
 * @param schema              Lock table schema.
 * @param r2dbc               Dedicated connection of the lock manager.
 */
@ConfigurationProperties("pg-advisory")
public record PgAdvisoryProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("advisory_lock") String tableName,
        @DefaultValue("pg-advisory") String applicationName,
        @DefaultValue("30s") Duration shutdownLockTimeout,
        @DefaultValue("1s") Duration heartbeatInterval,
        @DefaultValue("5s") Duration heartbeatTimeout,
        @DefaultValue Schema schema,
        @DefaultValue R2dbc r2dbc) {

    /**
     * @param autoCreate Whether to create the lock table on start when it does not exist. Otherwise create it with
     *                   classpath:db/pg-advisory/V1__advisory_lock.sql.
     */
    public record Schema(@DefaultValue("false") boolean autoCreate) {
    }

    /**
     * Without a URL the lock connection uses the application's connection details (spring.r2dbc.*), never its pool.
     * The application's ConnectionFactoryOptionsBuilderCustomizer beans apply either way.
     *
     * @param url      R2DBC URL of the lock connection, an r2dbc:pool: prefix is ignored.
     * @param username Login username of the lock connection.
     * @param password Login password of the lock connection.
     */
    public record R2dbc(@Nullable String url, @Nullable String username, @Nullable String password) {
    }

    /**
     * The {@link LockManagerSettings} of these properties.
     */
    public LockManagerSettings toSettings() {
        return LockManagerSettings.builder()
                .tableName(tableName)
                .applicationName(applicationName)
                .autoCreateSchema(schema.autoCreate())
                .shutdownLockTimeout(shutdownLockTimeout)
                .heartbeatInterval(heartbeatInterval)
                .heartbeatTimeout(heartbeatTimeout)
                .build();
    }
}
