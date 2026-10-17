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
 * @param retryMaxAttempts    how often an operation retries to get the lock connection, which serves one at a time
 * @param retryMinBackoff     first delay between the retries
 * @param retryMaxBackoff     maximal delay between the retries
 * @param retryJitter         jitter factor of the delays, 0 to 1
 * @param shutdownLockTimeout how long closing the lock manager waits for the lock connection
 */
public record LockManagerSettings(
        String tableName,
        String applicationName,
        boolean autoCreateSchema,
        long retryMaxAttempts,
        Duration retryMinBackoff,
        Duration retryMaxBackoff,
        double retryJitter,
        Duration shutdownLockTimeout) {

    private static final Pattern TABLE_NAME = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    public LockManagerSettings {
        if (!TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name [" + tableName + "]");
        }
        Objects.requireNonNull(applicationName, "applicationName");
        Objects.requireNonNull(retryMinBackoff, "retryMinBackoff");
        Objects.requireNonNull(retryMaxBackoff, "retryMaxBackoff");
        Objects.requireNonNull(shutdownLockTimeout, "shutdownLockTimeout");
    }

    public static LockManagerSettings defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {

        private String tableName = "advisory_lock";
        private String applicationName = "pg-advisory";
        private boolean autoCreateSchema;
        private long retryMaxAttempts = 100;
        private Duration retryMinBackoff = Duration.ofMillis(10);
        private Duration retryMaxBackoff = Duration.ofMillis(500);
        private double retryJitter = 0.5;
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

        public Builder retryMaxAttempts(long retryMaxAttempts) {
            this.retryMaxAttempts = retryMaxAttempts;
            return this;
        }

        public Builder retryMinBackoff(Duration retryMinBackoff) {
            this.retryMinBackoff = retryMinBackoff;
            return this;
        }

        public Builder retryMaxBackoff(Duration retryMaxBackoff) {
            this.retryMaxBackoff = retryMaxBackoff;
            return this;
        }

        public Builder retryJitter(double retryJitter) {
            this.retryJitter = retryJitter;
            return this;
        }

        public Builder shutdownLockTimeout(Duration shutdownLockTimeout) {
            this.shutdownLockTimeout = shutdownLockTimeout;
            return this;
        }

        public LockManagerSettings build() {
            return new LockManagerSettings(tableName, applicationName, autoCreateSchema, retryMaxAttempts, retryMinBackoff,
                    retryMaxBackoff, retryJitter, shutdownLockTimeout);
        }
    }
}
