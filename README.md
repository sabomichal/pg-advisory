# pg-advisory

[![Maven Central](https://img.shields.io/maven-central/v/com.github.sabomichal/pg-advisory?label=Maven%20Central)](https://central.sonatype.com/artifact/com.github.sabomichal/pg-advisory)
[![Build](https://github.com/sabomichal/pg-advisory/actions/workflows/maven.yml/badge.svg)](https://github.com/sabomichal/pg-advisory/actions/workflows/maven.yml)

Distributed locks on PostgreSQL session advisory locks, for Java, R2DBC and Reactor, with an optional
Spring Boot auto-configuration. All locks of an instance are held by one dedicated connection.

```java
lockManager.tryWithLock("order-42", handle -> processOrder(handle.key()))
        .flatMap(outcome -> switch (outcome) {
            case LockOutcome.Completed<Order>(var order) -> Mono.just(order);
            case LockOutcome.Busy<Order> _ -> Mono.empty();   // locked elsewhere or already running here
        });
```

## Setup

| pg-advisory | Spring Boot (starter) | Java | PostgreSQL R2DBC driver |
|-------------|-----------------------|------|-------------------------|
| `0.x`       | 4.1.x                 | 25+  | `org.postgresql:r2dbc-postgresql` |

With Spring Boot, add the starter:

```xml
<dependency>
    <groupId>com.github.sabomichal</groupId>
    <artifactId>pg-advisory-spring-boot-starter</artifactId>
    <version>${pg-advisory.version}</version>
</dependency>
```

Without Spring Boot, add the library alone; it depends on R2DBC and Reactor only:

```xml
<dependency>
    <groupId>com.github.sabomichal</groupId>
    <artifactId>pg-advisory</artifactId>
    <version>${pg-advisory.version}</version>
</dependency>
```

### Lock table

One row per key; its `id` is the advisory lock id, its `fencing_token` the token of the last lock of the
key. Create it with your migrations from
`classpath:db/pg-advisory/V1__advisory_lock.sql`, or let the library create it
(`pg-advisory.schema.auto-create=true`, `LockManagerSettings.autoCreateSchema`).

```sql
CREATE TABLE IF NOT EXISTS advisory_lock
(
    id            bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    lock_key      text   NOT NULL UNIQUE,
    fencing_token bigint NOT NULL DEFAULT 0
);
```

### Spring Boot

Adding the starter is enough: the auto-configuration registers a `LockManager` bean as soon as
there is something to connect it with. The lock connection is a connection of its own, never one from
the application's pool: from `pg-advisory.r2dbc.url` when it is set, otherwise with the
application's connection details (`spring.r2dbc.*`, Testcontainers or Docker Compose service
connections), or from your own `PgAdvisoryLockConnectionFactory` bean. Either way the application's
`ConnectionFactoryOptionsBuilderCustomizer` beans apply, so that it sees the same database and schema.

On start the lock manager verifies the lock table and fails when it is missing, unless
`pg-advisory.schema.auto-create=true`. The auto-configuration backs off when you define your own
`LockManager`, and `pg-advisory.enabled=false` switches it off.

`LockManagerListener` beans get the callbacks of the lock manager, and with a `MeterRegistry` it
records metrics, see [Observability](#observability).

### Plain Java

```java
var connectionFactory = ConnectionFactories.get("r2dbc:postgresql://user:password@localhost/app");  // unpooled
var settings = LockManagerSettings.builder().autoCreateSchema(true).build();
R2dbcLockManager.ensureTable(connectionFactory, settings).block();
var lockManager = new R2dbcLockManager(connectionFactory, settings,
        List.of(new MicrometerLockManagerListener(meterRegistry)));                               // optional
// ...
lockManager.close();
```

In a Spring application without Boot, return it from a `@Bean` method; Spring closes it on shutdown.

## API

| Method                                      | Does                                                                        |
|---------------------------------------------|-----------------------------------------------------------------------------|
| `tryWithLock(key, work)`                    | Locks `key` without waiting, runs `work`, unlocks; returns a `LockOutcome`. |
| `tryWithLock(key, onLock, onFinally, work)` | The same, with callbacks right before and after `work`.                     |

`tryWithLock` returns a sealed `LockManager.LockOutcome`:

- `Completed(value)`: the work finished with `value` while the lock was held. When the work completes
  empty, e.g. a `Mono<Void>`, `tryWithLock` completes empty as well.
- `Busy(sameInstance)`: the key is locked, by this instance when `sameInstance`, otherwise by another
  one; the work did not run and the callbacks were not called.

It fails with `LockFailedException` when the key could not be locked because of an error, e.g. the
database is unreachable, the lock manager is closed, or the fencing token could not be issued (the
lock is released before); the work did not run then either. Errors of
the work are propagated unchanged. A failed unlock does not fail it: the unlock is repeated with a
backoff from `unlock-retry-min-backoff` up to `unlock-retry-max-backoff` until it succeeds or the lock
connection is re-created, and `tryWithLock` completes only then.

The work and the callbacks get a `LockManager.LockHandle` with the key and the fencing token of the lock. The handle is also in
the Reactor context of the work, so code deep inside it finds the lock with `LockHandle.current()`.

### Fencing token

A lock does not guarantee that its holder still has it when a write arrives. When the lock connection
dies, PostgreSQL releases the lock, another instance takes the key over, and a write of the old holder
may land after the writes of the new one. The old holder does not know yet that its lock is gone, and
`onConnectionReset` may come after its last check. So the library issues a token, and the target of a
write checks it.

`LockHandle.fencingToken()` grows with every lock of the key, by any instance. Pass it along with the
writes, so that their target remembers the highest token and rejects older ones, in the same statement
as the write:

```sql
UPDATE job SET result = :result, fencing_token = :token
WHERE id = :id AND fencing_token <= :token   -- 0 rows: a newer holder of the key has written already
```

- **Check and write at once.** A check before the write does not suffice, a newer holder can write in
  between. Equal tokens pass, the same holder may write more than once.
- **Register the token before reading.** The target rejects the old holder only after it accepted a
  newer token, taking the lock alone does not stop the old holder. A holds the key with token 7 and
  stalls, its lock connection dies, B takes the key over with token 8 and reads the job. When A wakes
  up, its write with token 7 still lands, and B works on a state that is gone. B registers its token
  first, then A's write finds 8 and changes nothing:
  ```sql
  UPDATE job SET fencing_token = :token
  WHERE id = :id AND fencing_token <= :token   -- 0 rows: a newer holder of the key has registered already
  ```
- **Compare tokens of one key only.** Tokens of different keys, or of the same key in another lock
  table, are unrelated. A token grows only while the key's row in the lock table lives: deleting it, or
  the table, starts the key at 0 again, and a target that remembers a higher token then rejects every
  new holder. Keep the rows.

The token does not prove that the lock is still held, it lets the target of a write reject an older
holder.

## Observability

A `LockManagerListener` gets a callback for every lock attempt and every release:

| Callback            | When                                                                                     |
|---------------------|------------------------------------------------------------------------------------------|
| `onLocked`          | The key was locked, right before the work starts.                                        |
| `onBusy`            | The key was busy, with the `Busy` outcome.                                               |
| `onLockFailed`      | Locking failed with `LockFailedException`.                                               |
| `onReleased`        | After the lock was released, a failed unlock is repeated until then. Every `onLocked` is followed by exactly one. |
| `onConnectionReset` | The lock connection was found broken and re-created. Its locks are gone, so work running under them should stop. |

The lock callbacks get the time it took to lock, including the wait for the lock connection, and
`onReleased` the time the lock was held. The callbacks run on the locking path, so keep them fast and
non-blocking. A listener that throws is logged and affects neither the locking nor the other listeners.

With Spring Boot every `LockManagerListener` bean is registered, and the auto-configuration adds two:

- The callbacks are published as Spring application events, the sealed `PgAdvisoryEvents`: `Locked`,
  `Busy`, `LockFailed`, `Released` and `ConnectionReset`. They are published synchronously on the
  locking path as well.
- With a Micrometer `MeterRegistry`, e.g. from Spring Boot Actuator, `MicrometerLockManagerListener`
  records:

| Metric                          | Type    | Description                                                                                         |
|---------------------------------|---------|-----------------------------------------------------------------------------------------------------|
| `pg.advisory.lock`              | timer   | Time to lock a key, tag `outcome`: `locked`, `busy_same_instance`, `busy_other_instance`, `failed`. |
| `pg.advisory.hold`              | timer   | Time a lock was held.                                                                               |
| `pg.advisory.locks.held`        | gauge   | Locks held now.                                                                                     |
| `pg.advisory.connection.resets` | counter | Re-creations of a broken lock connection.                                                           |

Without Spring Boot, pass a `MicrometerLockManagerListener` to the constructor. `micrometer-core` is an
optional dependency. A health indicator comes later, reporting the state of the lock connection from the
heartbeat, see [ROADMAP.md](ROADMAP.md).

## Properties

All under `pg-advisory`.

| Property                                | Default              | Description                                                               |
|-----------------------------------------|----------------------|---------------------------------------------------------------------------|
| `enabled`                               | `true`               | Enables the auto-configuration.                                           |
| `table-name`                            | `advisory_lock`      | Lock table, optionally schema qualified.                                  |
| `application-name`                      | `pg-advisory`        | `application_name` of the lock connection, visible in `pg_stat_activity`. |
| `schema.auto-create`                    | `false`              | Creates the lock table on start when it does not exist.                   |
| `shutdown-lock-timeout`                 | `30s`                | How long closing waits for the queued operations, then it closes anyway.  |
| `heartbeat-interval`                    | `1s`                 | How often the lock connection is validated, a broken one is re-created.   |
| `heartbeat-timeout`                     | `5s`                 | How long a validation may take, then the connection counts as broken.     |
| `unlock-retry-min-backoff`              | `100ms`              | First delay before a failed unlock is repeated.                           |
| `unlock-retry-max-backoff`              | `5s`                 | Longest delay between the repeats of a failed unlock.                     |
| `r2dbc.url` / `username` / `password`   | *(`spring.r2dbc.*`)* | Dedicated, unpooled lock connection.                                      |

## How it works

`tryWithLock` locks the key twice:

1. **In memory**, so that the same instance cannot lock it again: session advisory locks are
   reentrant on the one lock connection. A key held here is `Busy(sameInstance = true)`.
2. **In PostgreSQL**, on the shared lock connection, in autocommit:
   ```sql
   INSERT INTO advisory_lock (lock_key) VALUES (:key) ON CONFLICT (lock_key) DO NOTHING;
   SELECT pg_try_advisory_lock(id) FROM advisory_lock WHERE lock_key = :key;  -- false → Busy(sameInstance = false)
   UPDATE advisory_lock SET fencing_token = fencing_token + 1 WHERE lock_key = :key RETURNING fencing_token;
   ```
   Not in a transaction: a session advisory lock would survive its rollback anyway. The new
   `fencing_token` is `LockHandle.fencingToken()`: higher than the token of every earlier lock of the
   key, by any instance. When it cannot be issued, the key is unlocked again.

Then `work` runs, and the lock is released with `pg_advisory_unlock`. A failed unlock is repeated
with a backoff, each time at the end of the queue, so that other locks go on, and the key stays busy
until it succeeds. The lock connection serves one operation at a time, the others wait in a FIFO queue.

A heartbeat validates the lock connection every `heartbeat-interval`, through the same queue. A
validation that does not answer within `heartbeat-timeout` counts as broken. A broken connection is
re-created, which drops its locks, and `LockManagerListener.onConnectionReset` is called. The
operations still queued for it end with it: a lock fails with `LockFailedException`, an unlock has
nothing left to release.

## Limitations

- **Fencing is up to the application.** When the lock connection dies, PostgreSQL releases its locks
  and another instance can take over, while work of this instance may still be running and committing.
  The heartbeat notices it within `heartbeat-interval` and calls `LockManagerListener.onConnectionReset`,
  the work itself is not told. The library issues the fencing token, the target of a write has to check
  it, see [Fencing token](#fencing-token).
- Known weaknesses of the current implementation, e.g. a reset lets this instance lock a key again
  while the old work under it still runs, and how they are going to be fixed step by step: see
  [ROADMAP.md](ROADMAP.md).
- The lock connection must reach PostgreSQL directly or through a pooler in session mode, and must
  not come from a pool. `idle_session_timeout` of the database must be longer than `heartbeat-interval`.

## Build

```shell
mvn verify      # needs Docker for Testcontainers
```

JMH benchmarks are in the tests of `pg-advisory`, in `R2dbcLockManagerBenchmark`: `throughput` (lock/unlock cycles
per second with 1, 16 or 256 at once) and `latency1`, `latency16`, `latency256` (latency percentiles of one cycle
among 1, 16 or 256 callers). Run its `main` in the IDE, with JMH options such as `latency -wi 1 -i 2`. They need Docker too.

See [DEPLOYMENT.md](DEPLOYMENT.md) for releasing to Maven Central.

## License

[Apache 2.0](LICENSE)
