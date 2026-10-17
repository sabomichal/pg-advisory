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
 * @param shutdownLockTimeout How long closing the lock manager waits for the lock connection.
 * @param schema              Lock table schema.
 * @param retry               Waiting for the lock connection, which serves one operation at a time.
 * @param r2dbc               Dedicated connection of the lock manager.
 */
@ConfigurationProperties("pg-advisory")
public record PgAdvisoryProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("advisory_lock") String tableName,
        @DefaultValue("pg-advisory") String applicationName,
        @DefaultValue("30s") Duration shutdownLockTimeout,
        @DefaultValue Schema schema,
        @DefaultValue Retry retry,
        @DefaultValue R2dbc r2dbc) {

    /**
     * @param autoCreate Whether to create the lock table on start when it does not exist; otherwise create it with
     *                   classpath:db/pg-advisory/V1__advisory_lock.sql.
     */
    public record Schema(@DefaultValue("false") boolean autoCreate) {
    }

    /**
     * @param maxAttempts How often an operation retries to get the lock connection.
     * @param minBackoff  First delay between the retries.
     * @param maxBackoff  Maximal delay between the retries.
     * @param jitter      Jitter factor of the delays, 0 to 1.
     */
    public record Retry(@DefaultValue("100") long maxAttempts,
                        @DefaultValue("10ms") Duration minBackoff,
                        @DefaultValue("500ms") Duration maxBackoff,
                        @DefaultValue("0.5") double jitter) {
    }

    /**
     * Dedicated, unpooled connection of the lock manager. When the URL is not set, it connects with the
     * application's connection details (spring.r2dbc.*), but never through its pool. The application's
     * ConnectionFactoryOptionsBuilderCustomizer beans apply either way.
     *
     * @param url      R2DBC URL of the lock connection; an r2dbc:pool: prefix is ignored.
     * @param username Login username of the lock connection.
     * @param password Login password of the lock connection.
     */
    public record R2dbc(@Nullable String url, @Nullable String username, @Nullable String password) {
    }

    public LockManagerSettings toSettings() {
        return LockManagerSettings.builder()
                .tableName(tableName)
                .applicationName(applicationName)
                .autoCreateSchema(schema.autoCreate())
                .retryMaxAttempts(retry.maxAttempts())
                .retryMinBackoff(retry.minBackoff())
                .retryMaxBackoff(retry.maxBackoff())
                .retryJitter(retry.jitter())
                .shutdownLockTimeout(shutdownLockTimeout)
                .build();
    }
}
