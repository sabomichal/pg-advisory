package com.github.sabomichal.pgadvisory;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Settings of the {@link LockManager}.
 *
 * @param tableName             lock table, optionally schema qualified
 * @param applicationName       {@code application_name} of the lock connection, visible in {@code pg_stat_activity}
 * @param autoCreateSchema      create the lock table when it does not exist
 * @param shutdownLockTimeout   how long closing the lock manager waits for the queued operations, then it closes the
 *                              lock connection anyway
 * @param heartbeatInterval     how often the lock connection is validated, a broken one is re-created
 * @param heartbeatTimeout      how long a validation of the lock connection may take, then it counts as broken
 * @param unlockRetryMinBackoff first delay before a failed unlock is repeated
 * @param unlockRetryMaxBackoff longest delay between the repeats of a failed unlock
 */
public record LockManagerSettings(
        String tableName,
        String applicationName,
        boolean autoCreateSchema,
        Duration shutdownLockTimeout,
        Duration heartbeatInterval,
        Duration heartbeatTimeout,
        Duration unlockRetryMinBackoff,
        Duration unlockRetryMaxBackoff) {

    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    /**
     * {@code tableName} goes into the SQL as is, so it must be an identifier, optionally schema qualified.
     */
    public LockManagerSettings {
        if (!TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name [" + tableName + "]");
        }
        Objects.requireNonNull(applicationName, "applicationName");
        Objects.requireNonNull(shutdownLockTimeout, "shutdownLockTimeout");
        Objects.requireNonNull(heartbeatInterval, "heartbeatInterval");
        Objects.requireNonNull(heartbeatTimeout, "heartbeatTimeout");
        Objects.requireNonNull(unlockRetryMinBackoff, "unlockRetryMinBackoff");
        Objects.requireNonNull(unlockRetryMaxBackoff, "unlockRetryMaxBackoff");
    }

    /**
     * Settings of a new {@link #builder()}.
     */
    public static LockManagerSettings defaults() {
        return builder().build();
    }

    /**
     * Starts from the defaults: table {@code advisory_lock}, application name {@code pg-advisory}, no auto-create,
     * 30s shutdown lock timeout, a heartbeat every 1s with a 5s timeout, and a failed unlock repeated after 100ms,
     * backing off up to 5s.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Each setter sets the component of the same name, see {@link LockManagerSettings}.
     */
    public static final class Builder {

        private String tableName = "advisory_lock";
        private String applicationName = "pg-advisory";
        private boolean autoCreateSchema;
        private Duration shutdownLockTimeout = Duration.ofSeconds(30);
        private Duration heartbeatInterval = Duration.ofSeconds(1);
        private Duration heartbeatTimeout = Duration.ofSeconds(5);
        private Duration unlockRetryMinBackoff = Duration.ofMillis(100);
        private Duration unlockRetryMaxBackoff = Duration.ofSeconds(5);

        private Builder() {
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder applicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public Builder autoCreateSchema(boolean autoCreateSchema) {
            this.autoCreateSchema = autoCreateSchema;
            return this;
        }

        public Builder shutdownLockTimeout(Duration shutdownLockTimeout) {
            this.shutdownLockTimeout = shutdownLockTimeout;
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

        public Builder unlockRetryMinBackoff(Duration unlockRetryMinBackoff) {
            this.unlockRetryMinBackoff = unlockRetryMinBackoff;
            return this;
        }

        public Builder unlockRetryMaxBackoff(Duration unlockRetryMaxBackoff) {
            this.unlockRetryMaxBackoff = unlockRetryMaxBackoff;
            return this;
        }

        public LockManagerSettings build() {
            return new LockManagerSettings(tableName, applicationName, autoCreateSchema, shutdownLockTimeout,
                    heartbeatInterval, heartbeatTimeout, unlockRetryMinBackoff, unlockRetryMaxBackoff);
        }
    }
}
