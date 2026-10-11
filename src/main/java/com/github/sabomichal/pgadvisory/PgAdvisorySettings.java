package com.github.sabomichal.pgadvisory;

import com.github.sabomichal.pgadvisory.PgAdvisory.LostLockPolicy;

import java.net.InetAddress;
import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Settings of {@link PgAdvisory}.
 *
 * @param tableName           lock table, optionally schema qualified
 * @param owner               recorded in the lock table and in {@code application_name} of the lock session
 * @param fenceNamespace      first key of the two-int advisory lock used as the takeover barrier
 * @param autoCreateSchema    create the lock table on start when it does not exist
 * @param heartbeatInterval   how often the lock session is validated
 * @param heartbeatTimeout    how long a validation may take
 * @param barrierTimeout      how long a new holder waits for in-flight commits of the previous holder
 * @param reconnectMinBackoff first delay between reconnect attempts
 * @param reconnectMaxBackoff maximal delay between reconnect attempts
 * @param lostLockPolicy      default policy for running work when the lock session is lost
 * @param shutdownTimeout     how long {@link PgAdvisory#stop()} waits for running work
 * @param lifecyclePhase      Spring {@code SmartLifecycle} phase; low, so the lock session starts early and stops late
 */
public record PgAdvisorySettings(
        String tableName,
        String owner,
        int fenceNamespace,
        boolean autoCreateSchema,
        Duration heartbeatInterval,
        Duration heartbeatTimeout,
        Duration barrierTimeout,
        Duration reconnectMinBackoff,
        Duration reconnectMaxBackoff,
        LostLockPolicy lostLockPolicy,
        Duration shutdownTimeout,
        int lifecyclePhase) {

    public static final String DEFAULT_TABLE_NAME = "advisory_lock";
    /** "pgad" */
    public static final int DEFAULT_FENCE_NAMESPACE = 0x70676164;

    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    public PgAdvisorySettings {
        if (!TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name [" + tableName + "]");
        }
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(lostLockPolicy, "lostLockPolicy");
    }

    public static PgAdvisorySettings defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public Builder toBuilder() {
        return new Builder()
                .tableName(tableName)
                .owner(owner)
                .fenceNamespace(fenceNamespace)
                .autoCreateSchema(autoCreateSchema)
                .heartbeatInterval(heartbeatInterval)
                .heartbeatTimeout(heartbeatTimeout)
                .barrierTimeout(barrierTimeout)
                .reconnectMinBackoff(reconnectMinBackoff)
                .reconnectMaxBackoff(reconnectMaxBackoff)
                .lostLockPolicy(lostLockPolicy)
                .shutdownTimeout(shutdownTimeout)
                .lifecyclePhase(lifecyclePhase);
    }

    /** {@code hostname:pid} */
    public static String defaultOwner() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        return host + ":" + ProcessHandle.current().pid();
    }

    public static final class Builder {
        private String tableName = DEFAULT_TABLE_NAME;
        private String owner = defaultOwner();
        private int fenceNamespace = DEFAULT_FENCE_NAMESPACE;
        private boolean autoCreateSchema = false;
        private Duration heartbeatInterval = Duration.ofSeconds(5);
        private Duration heartbeatTimeout = Duration.ofSeconds(5);
        private Duration barrierTimeout = Duration.ofSeconds(10);
        private Duration reconnectMinBackoff = Duration.ofMillis(500);
        private Duration reconnectMaxBackoff = Duration.ofSeconds(30);
        private LostLockPolicy lostLockPolicy = LostLockPolicy.CANCEL_WORK;
        private Duration shutdownTimeout = Duration.ofSeconds(30);
        private int lifecyclePhase = Integer.MAX_VALUE - 4096;

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder owner(String owner) {
            this.owner = owner;
            return this;
        }

        public Builder fenceNamespace(int fenceNamespace) {
            this.fenceNamespace = fenceNamespace;
            return this;
        }

        public Builder autoCreateSchema(boolean autoCreateSchema) {
            this.autoCreateSchema = autoCreateSchema;
            return this;
        }

        public Builder heartbeatInterval(Duration heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder heartbeatTimeout(Duration heartbeatTimeout) {
            this.heartbeatTimeout = heartbeatTimeout;
            return this;
        }

        public Builder barrierTimeout(Duration barrierTimeout) {
            this.barrierTimeout = barrierTimeout;
            return this;
        }

        public Builder reconnectMinBackoff(Duration reconnectMinBackoff) {
            this.reconnectMinBackoff = reconnectMinBackoff;
            return this;
        }

        public Builder reconnectMaxBackoff(Duration reconnectMaxBackoff) {
            this.reconnectMaxBackoff = reconnectMaxBackoff;
            return this;
        }

        public Builder lostLockPolicy(LostLockPolicy lostLockPolicy) {
            this.lostLockPolicy = lostLockPolicy;
            return this;
        }

        public Builder shutdownTimeout(Duration shutdownTimeout) {
            this.shutdownTimeout = shutdownTimeout;
            return this;
        }

        public Builder lifecyclePhase(int lifecyclePhase) {
            this.lifecyclePhase = lifecyclePhase;
            return this;
        }

        public PgAdvisorySettings build() {
            return new PgAdvisorySettings(tableName, owner, fenceNamespace, autoCreateSchema, heartbeatInterval,
                    heartbeatTimeout, barrierTimeout, reconnectMinBackoff, reconnectMaxBackoff, lostLockPolicy,
                    shutdownTimeout, lifecyclePhase);
        }
    }
}
