package com.github.sabomichal.pgadvisory;

import java.time.Duration;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Settings of the {@link LockManager}.
 *
 * @param tableName           lock table, optionally schema qualified
 * @param applicationName     {@code application_name} of the lock connection, visible in {@code pg_stat_activity}
 * @param autoCreateSchema    create the lock table when it does not exist
 * @param shutdownLockTimeout how long closing the lock manager waits for the queued operations, then it closes the
 *                            lock connection anyway
 */
public record LockManagerSettings(
        String tableName,
        String applicationName,
        boolean autoCreateSchema,
        Duration shutdownLockTimeout) {

    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    /** {@code tableName} goes into the SQL as is, so it must be an identifier, optionally schema qualified. */
    public LockManagerSettings {
        if (!TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name [" + tableName + "]");
        }
        Objects.requireNonNull(applicationName, "applicationName");
        Objects.requireNonNull(shutdownLockTimeout, "shutdownLockTimeout");
    }

    /** Settings of a new {@link #builder()}. */
    public static LockManagerSettings defaults() {
        return builder().build();
    }

    /**
     * Starts from the defaults: table {@code advisory_lock}, application name {@code pg-advisory}, no auto-create and
     * 30 s shutdown lock timeout.
     */
    public static Builder builder() {
        return new Builder();
    }

    /** Each setter sets the component of the same name, see {@link LockManagerSettings}. */
    public static final class Builder {

        private String tableName = "advisory_lock";
        private String applicationName = "pg-advisory";
        private boolean autoCreateSchema;
        private Duration shutdownLockTimeout = Duration.ofSeconds(30);

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

        public LockManagerSettings build() {
            return new LockManagerSettings(tableName, applicationName, autoCreateSchema, shutdownLockTimeout);
        }
    }
}
