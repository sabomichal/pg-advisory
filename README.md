# pg-advisory

[![Maven Central](https://img.shields.io/maven-central/v/com.github.sabomichal/pg-advisory?label=Maven%20Central)](https://central.sonatype.com/artifact/com.github.sabomichal/pg-advisory)
[![Build](https://github.com/sabomichal/pg-advisory/actions/workflows/maven.yml/badge.svg)](https://github.com/sabomichal/pg-advisory/actions/workflows/maven.yml)

Distributed locks on PostgreSQL session advisory locks, for Java, R2DBC and Reactor, with an optional
Spring Boot auto-configuration. All locks of an instance are held by one dedicated connection.

```java
lockManager.tryWithLock("order-42", key -> processOrder(key))
        .onErrorResume(LockOperationException.class, busy -> Mono.empty());   // locked elsewhere
```

## Setup

| pg-advisory | Spring Boot | Java | PostgreSQL R2DBC driver |
|-------------|-------------|------|-------------------------|
| `0.x`       | 4.0.x       | 25+  | `org.postgresql:r2dbc-postgresql` |

```xml
<dependency>
    <groupId>com.github.sabomichal</groupId>
    <artifactId>pg-advisory</artifactId>
    <version>${pg-advisory.version}</version>
</dependency>
```

Spring and Spring Boot are optional dependencies; the integration switches on when they are on the
classpath.

### Lock table

One row per key; its `id` is the advisory lock id. Create it with your migrations from
`classpath:db/pg-advisory/V1__advisory_lock.sql`, or let the library create it
(`pg-advisory.schema.auto-create=true`, `LockManagerSettings.autoCreateSchema`).

```sql
CREATE TABLE IF NOT EXISTS advisory_lock
(
    id       bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lock_key text   NOT NULL UNIQUE
);
```

### Spring Boot

Adding the dependency is enough: the auto-configuration registers a `LockManager` bean as soon as
there is something to connect it with. The lock connection is a connection of its own, never one from
the application's pool: from `pg-advisory.r2dbc.url` when it is set, otherwise with the
application's connection details (`spring.r2dbc.*`, Testcontainers or Docker Compose service
connections), or from your own `PgAdvisoryLockConnectionFactory` bean. Either way the application's
`ConnectionFactoryOptionsBuilderCustomizer` beans apply, so that it sees the same database and schema.

On start the lock manager verifies the lock table and fails when it is missing, unless
`pg-advisory.schema.auto-create=true`. The auto-configuration backs off when you define your own
`LockManager`, and `pg-advisory.enabled=false` switches it off.

`ConnectionResetListener` beans are called when the lock connection was found broken and re-created:
its locks are gone, so work running under them should stop.

### Spring without Boot

Add `@EnablePgAdvisory` and a `PgAdvisoryLockConnectionFactory` bean with an unpooled
`ConnectionFactory`, optionally a `LockManagerSettings` bean.

### Plain Java

```java
var lockManager = new R2dbcLockManager(
        ConnectionFactories.get("r2dbc:postgresql://user:password@localhost/app"),   // unpooled
        LockManagerSettings.builder().autoCreateSchema(true).build(),
        () -> log.warn("Lock connection re-created, all locks are gone"));
lockManager.ensureTable().block();
// ...
lockManager.close();
```

## API

| Method                                                | Does                                                                    |
|-------------------------------------------------------|-------------------------------------------------------------------------|
| `tryWithLock(key, function)`                          | Locks `key` without waiting, runs `function`, unlocks.                  |
| `tryWithLock(key, onLock, onFinally, function)`       | The same, with callbacks right before and after `function`.             |
| `validateDbConnection()`                              | Checks the lock connection and re-creates it when it is broken.         |

A key that is locked fails with `LockOperationException`; `isSameInstance()` tells whether this
instance holds it.

## Properties

All under `pg-advisory`.

| Property                                | Default              | Description                                                               |
|-----------------------------------------|----------------------|---------------------------------------------------------------------------|
| `enabled`                               | `true`               | Enables the auto-configuration.                                           |
| `table-name`                            | `advisory_lock`      | Lock table, optionally schema qualified.                                  |
| `application-name`                      | `pg-advisory`        | `application_name` of the lock connection, visible in `pg_stat_activity`. |
| `schema.auto-create`                    | `false`              | Creates the lock table on start when it does not exist.                   |
| `retry.max-attempts`                    | `100`                | How often an operation retries to get the lock connection.                |
| `retry.min-backoff` / `max-backoff`     | `10ms` / `500ms`     | Delays between the retries.                                               |
| `retry.jitter`                          | `0.5`                | Jitter factor of the delays.                                              |
| `initial-lock-timeout`                  | `30s`                | How long creating the lock manager waits for the lock connection.         |
| `shutdown-lock-timeout`                 | `30s`                | How long closing the lock manager waits for the lock connection.          |
| `r2dbc.url` / `username` / `password`   | *(`spring.r2dbc.*`)* | Dedicated, unpooled lock connection.                                      |

## How it works

`tryWithLock` locks the key twice:

1. **In memory**, so that the same instance cannot lock it again: session advisory locks are
   reentrant on the one lock connection.
2. **In PostgreSQL**, on the shared lock connection:
   ```sql
   BEGIN;
   INSERT INTO advisory_lock (lock_key) VALUES (:key) ON CONFLICT (lock_key) DO NOTHING;
   SELECT pg_try_advisory_lock(id) FROM advisory_lock WHERE lock_key = :key;  -- false → LockOperationException
   COMMIT;
   ```

Then `function` runs, and the lock is released with `pg_advisory_unlock`. The lock connection serves
one operation at a time; others wait with backoff (`retry.*`).

## Limitations

- **No fencing.** When the lock connection dies, PostgreSQL releases its locks and another instance
  can take over, while work of this instance may still be running and committing. Only
  `validateDbConnection` notices it, and then calls the `ConnectionResetListener`s.
- Known weaknesses of the current implementation, e.g. cancelling during locking or the retry
  limit of the lock connection under load, and how they are going to be fixed step by step:
  see [ROADMAP.md](ROADMAP.md).
- The lock connection must reach PostgreSQL directly or through a pooler in session mode, and must
  not come from a pool.

## Build

```shell
mvn verify      # needs Docker for Testcontainers
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for releasing to Maven Central.

## License

[Apache 2.0](LICENSE)
