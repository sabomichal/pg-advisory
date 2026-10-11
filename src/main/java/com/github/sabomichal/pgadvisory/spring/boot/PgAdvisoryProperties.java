package com.github.sabomichal.pgadvisory.spring.boot;

import com.github.sabomichal.pgadvisory.PgAdvisory.LostLockPolicy;
import com.github.sabomichal.pgadvisory.PgAdvisorySettings;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;

/**
 * Configuration of pg-advisory. The defaults are the same as {@link PgAdvisorySettings#defaults()}.
 *
 * @param enabled           Whether to enable pg-advisory.
 * @param tableName         Lock table, optionally schema qualified.
 * @param owner             Name of this instance, recorded in the lock table and in the application_name of the
 *                          lock session; defaults to hostname:pid.
 * @param fenceNamespace    First key of the two-int advisory lock used as the takeover barrier.
 * @param heartbeatInterval How often the lock session is validated.
 * @param heartbeatTimeout  How long a validation of the lock session may take.
 * @param barrierTimeout    How long a new holder waits for in-flight commits of the previous holder before giving up.
 * @param lostLockPolicy            What happens to running work when the lock session is lost.
 * @param shutdownTimeout   How long stopping waits for running work before cancelling it.
 * @param schema            Lock table schema.
 * @param r2dbc             Dedicated connection of the lock session.
 * @param reconnect         Reconnecting of a lost lock session.
 * @param fencing           Fencing of transactions.
 * @param lifecycle         Lifecycle of the lock session.
 */
@ConfigurationProperties("spring.pg-advisory")
public record PgAdvisoryProperties(
        @DefaultValue("false") boolean enabled,
        @DefaultValue(PgAdvisorySettings.DEFAULT_TABLE_NAME) String tableName,
        @Nullable String owner,
        @DefaultValue("1885823332") int fenceNamespace,
        @DefaultValue("5s") Duration heartbeatInterval,
        @DefaultValue("5s") Duration heartbeatTimeout,
        @DefaultValue("10s") Duration barrierTimeout,
        @DefaultValue("CANCEL_WORK") LostLockPolicy lostLockPolicy,
        @DefaultValue("30s") Duration shutdownTimeout,
        @DefaultValue Schema schema,
        @DefaultValue R2dbc r2dbc,
        @DefaultValue Reconnect reconnect,
        @DefaultValue Fencing fencing,
        @DefaultValue Lifecycle lifecycle) {

    /**
     * @param autoCreate Whether to create the lock table on start when it does not exist; otherwise create it with
     *                   classpath:db/pg-advisory/V1__advisory_lock.sql.
     */
    public record Schema(@DefaultValue("false") boolean autoCreate) {
    }

    /**
     * Dedicated, unpooled connection of the lock session. When the URL is not set, the lock session connects with
     * the application's connection details (spring.r2dbc.*), but never through its pool. The application's
     * ConnectionFactoryOptionsBuilderCustomizer beans apply either way.
     *
     * @param url      R2DBC URL of the lock session; an r2dbc:pool: prefix is ignored.
     * @param username Login username of the lock session.
     * @param password Login password of the lock session.
     */
    public record R2dbc(@Nullable String url, @Nullable String username, @Nullable String password) {
    }

    /**
     * @param minBackoff First delay between reconnect attempts.
     * @param maxBackoff Maximal delay between reconnect attempts.
     */
    public record Reconnect(@DefaultValue("500ms") Duration minBackoff, @DefaultValue("30s") Duration maxBackoff) {
    }

    /**
     * @param enabled Whether to register FencingR2dbcTransactionManager as the ReactiveTransactionManager.
     */
    public record Fencing(@DefaultValue("true") boolean enabled) {
    }

    /**
     * @param phase SmartLifecycle phase of the lock session; low, so that it starts early and stops late.
     */
    public record Lifecycle(@DefaultValue("2147479551") int phase) {
    }

    public PgAdvisorySettings toSettings() {
        var settings = PgAdvisorySettings.builder()
                .tableName(tableName)
                .fenceNamespace(fenceNamespace)
                .autoCreateSchema(schema.autoCreate())
                .heartbeatInterval(heartbeatInterval)
                .heartbeatTimeout(heartbeatTimeout)
                .barrierTimeout(barrierTimeout)
                .reconnectMinBackoff(reconnect.minBackoff())
                .reconnectMaxBackoff(reconnect.maxBackoff())
                .lostLockPolicy(lostLockPolicy)
                .shutdownTimeout(shutdownTimeout)
                .lifecyclePhase(lifecycle.phase());
        if (owner != null) {
            settings.owner(owner);
        }
        return settings.build();
    }
}
