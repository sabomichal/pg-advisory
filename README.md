# pg-advisory

[![Maven Central](https://img.shields.io/maven-central/v/com.github.sabomichal/pg-advisory?label=Maven%20Central)](https://central.sonatype.com/artifact/com.github.sabomichal/pg-advisory)
[![Build](https://github.com/sabomichal/pg-advisory/actions/workflows/maven.yml/badge.svg)](https://github.com/sabomichal/pg-advisory/actions/workflows/maven.yml)

Long-lived distributed locks on PostgreSQL session advisory locks, for R2DBC and Spring Boot.
All locks of an instance are held by one dedicated connection, the work runs on pooled
connections, and fencing makes sure a holder that lost its lock cannot commit
after the next holder has started.

```java
pgAdvisory.withLock(betslipId, handle -> processBetslip(betslipId))
        .map(outcome -> switch (outcome) {
            case LockOutcome.Completed<Result> completed -> completed.value();
            case LockOutcome.Busy<Result> busy -> skip("owned by " + busy.owner());
            case LockOutcome.HeldLocally<Result> heldLocally -> skip("already running here");
            case LockOutcome.Lost<Result> lost -> skip("lock lost, writes were fenced off");
            case LockOutcome.Unavailable<Result> unavailable -> skip("lock session unavailable");
        });
```

## Why

Some locks must outlive many transactions and non-database work, such as calls to other
services or waiting for events. Such a lock cannot live in a transaction, and one connection
per held lock would exhaust the pool. pg-advisory keeps all locks of an instance on one
dedicated connection, the **lock session**:

```
 instance A                                      PostgreSQL
 ┌──────────────────────────┐
 │ lock session (unpooled)  │──── pg_try_advisory_lock(id) ─────►  session advisory locks
 │                          │
 │ work on pooled conns  ───│──── BEGIN … fence … COMMIT ───────►  your tables
 └──────────────────────────┘
```

That split has a price. When the lock session dies, PostgreSQL releases its locks at once,
but the work keeps running on other connections and another instance may take over.
pg-advisory closes this window with fencing.

## Features

- **One connection for all locks**: the lock session holds every lock of the instance;
  heartbeats detect a dead session and it reconnects with backoff.
- **Fencing**: every acquisition gets a new, higher fencing token (the `generation` of the key's
  row), and every work transaction checks it right before `COMMIT`. A stale holder rolls back.
- **No lost writes in the takeover window**: a new holder waits for in-flight commits of the
  previous one (a shared/exclusive barrier lock), so no write of the old holder lands after the
  new holder has started.
- **Typed outcomes**: `withLock` returns a sealed `LockOutcome`, so the compiler checks that
  `Busy`, `Lost` and the rest are handled.
- **Spring Boot starter**: auto-configured `PgAdvisory`, fencing transaction manager,
  graceful shutdown, events, Micrometer metrics and a health indicator.

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

```groovy
implementation "com.github.sabomichal:pg-advisory:${pgAdvisoryVersion}"
```

Spring, Spring Boot and Micrometer are optional dependencies; the integrations switch on
when they are on the classpath.

### Lock table

One row per key:

```sql
CREATE TABLE IF NOT EXISTS advisory_lock
(
    id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, -- advisory lock id
    lock_key   text   NOT NULL UNIQUE,
    generation bigint NOT NULL DEFAULT 0,                       -- fencing token
    owner      text                                             -- owner of the last acquisition
);
```

Either let the library create it (`spring.pg-advisory.schema.auto-create=true`) or add
`classpath:db/pg-advisory/V1__advisory_lock.sql` to your migrations.

### Spring Boot

```yaml
spring:
  pg-advisory:
    enabled: true
```

This registers:

- a `PgAdvisory<Object>` bean;
- a `SmartLifecycle` that opens the lock session on start, and on stop drains running work and
  releases the locks;
- `FencingR2dbcTransactionManager` as the `ReactiveTransactionManager`;
- Spring events (`PgAdvisoryEvents`);
- Micrometer metrics (`pg.advisory.*`), when a `MeterRegistry` is present;
- a health indicator `pgAdvisory`.

The lock session always gets a connection of its own, never one from the application's pool: from
`spring.pg-advisory.r2dbc.url` when it is set, otherwise with the application's connection details
(`spring.r2dbc.*`, Testcontainers or Docker Compose service connections). Either way the
application's `ConnectionFactoryOptionsBuilderCustomizer` beans apply, so that the lock session sees
the lock table in the same schema as the application.

### Spring without Boot

Add `@EnablePgAdvisory` and a `PgAdvisoryLockConnectionFactory` bean with an unpooled
`ConnectionFactory` for the lock session, optionally a `PgAdvisorySettings` bean, and register a
`FencingR2dbcTransactionManager` yourself.

### Plain Java

```java
PgAdvisory<UUID> pgAdvisory = R2dbcPgAdvisory.<UUID>builder(unpooledConnectionFactory)
        .workConnectionFactory(pool)
        .settings(PgAdvisorySettings.builder().owner("worker-1").autoCreateSchema(true).build())
        .build();
pgAdvisory.start().block();
```

### Kotlin

With `kotlinx-coroutines-reactor` on the classpath, the work can be a suspending function:

```kotlin
val outcome = pgAdvisory.withLockAndAwait(key) {          // this: LockHandle
    transactionalOperator.executeAndAwait { repository.save(item) }   // fenced like in Java
}
when (outcome) {
    is LockOutcome.Completed -> log.info("done with fencing token ${outcome.fencingToken()}")
    is LockOutcome.Busy -> log.info("held by ${outcome.owner()}")
    else -> log.warn("not done: $outcome")
}
```

Cancelling the calling coroutine cancels the work and releases the lock. `startAndAwait`,
`stopAndAwait`, `fenceAndAwait`, `verifyAndAwait` and `currentLockHandle()` cover the rest.

## Properties

All optional, under `spring.pg-advisory`. IDEs complete and document them from
`META-INF/spring-configuration-metadata.json`.

| Key                                    | Default                    | Description                                                                 |
|----------------------------------------|----------------------------|-----------------------------------------------------------------------------|
| `enabled`                              | `false`                    | Turns the auto-configuration on.                                            |
| `table-name`                           | `advisory_lock`            | Lock table, optionally schema qualified.                                    |
| `owner`                                | `hostname:pid`             | Name of this instance, in the lock table and in the lock session's `application_name`. |
| `schema.auto-create`                   | `false`                    | Creates the lock table on start when it does not exist.                     |
| `heartbeat-interval`                   | `5s`                       | How often the lock session is validated.                                    |
| `heartbeat-timeout`                    | `5s`                       | How long one validation may take.                                           |
| `barrier-timeout`                      | `10s`                      | How long a new holder waits for in-flight commits of the previous holder.   |
| `lost-lock-policy`                     | `CANCEL_WORK`              | `CANCEL_WORK` cancels running work, `NOTIFY` lets it run (still fenced).    |
| `shutdown-timeout`                     | `30s`                      | How long stopping waits for running work before cancelling it.              |
| `fence-namespace`                      | `1885823332`               | First key of the two-int advisory lock used as the takeover barrier.        |
| `fencing.enabled`                      | `true`                     | Registers `FencingR2dbcTransactionManager`.                                 |
| `r2dbc.url` / `username` / `password`  | *(`spring.r2dbc.*`)*       | Dedicated, unpooled connection of the lock session.                         |
| `reconnect.min-backoff` / `max-backoff`| `500ms` / `30s`            | Delays between reconnect attempts.                                          |
| `lifecycle.phase`                      | `Integer.MAX_VALUE - 4096` | `SmartLifecycle` phase; starts early, stops late.                           |

## How it works

Fencing has two parts: the **fencing token** is the `generation` column of the key's row, and
the **barrier** is a two-int advisory lock `(fence-namespace, barrierKey)` that a new holder takes
exclusively and every work transaction takes shared.

**Acquire**, first on the lock session, then on a pooled connection:

```sql
-- lock session, autocommit: nothing here waits for other holders
SELECT pg_try_advisory_lock(:id);                              -- false → Busy
-- pooled connection: wait for in-flight commits of the previous holder, then take the next token
BEGIN; SET LOCAL lock_timeout = :barrierTimeout;               -- timeout → ROLLBACK, unlock, Busy
SELECT pg_advisory_xact_lock(:namespace, :barrierKey);         -- exclusive
UPDATE advisory_lock SET generation = generation + 1, owner = :owner WHERE id = :id RETURNING generation;
-- lock session, same connection as pg_try_advisory_lock: still alive? otherwise ROLLBACK, Unavailable
SELECT 1;
COMMIT;
```

The lock is taken on the lock session and the token issued on a pooled connection, so the lock
session may die in between and another instance may take over first. The final check catches
that: a session advisory lock lives as long as its session, and the check runs while the
exclusive barrier keeps every later takeover waiting.

**Every work transaction**, right before `COMMIT`. `FencingR2dbcTransactionManager` does this
for you:

```sql
SELECT pg_advisory_xact_lock_shared(:namespace, :barrierKey);  -- held until COMMIT
SELECT generation FROM advisory_lock WHERE id = :id;           -- still mine? otherwise ROLLBACK
```

Each fencing check happens either before or after the moment a new holder takes over:

- **After the takeover:** the check sees the new generation and the transaction rolls back
  with `AdvisoryLockFencedException`.
- **Before the takeover:** the transaction already holds the shared barrier, so the new
  holder's exclusive barrier waits until it commits.

Under `READ COMMITTED` the check writes nothing and does not read `pg_locks`. Under
`REPEATABLE READ` and `SERIALIZABLE` the generation is read with `FOR SHARE`, because the
transaction's snapshot may be older than the takeover. Read-only transactions are not checked.

### Example: a takeover

Instance A holds key `k` at generation 5. Its lock session dies, so PostgreSQL releases the lock.
Two independent work transactions of A, **Invoice** and **Stock**, are still running on pooled
connections. A learns about the loss only from its heartbeat. Instance B now acquires `k`.

1. **Invoice checks before its commit** (A's pooled connection):
   ```sql
   SELECT pg_advisory_xact_lock_shared(ns, bk);  -- granted, nobody holds it exclusively
   SELECT generation FROM advisory_lock WHERE id = :id;  -- 5, still mine
   COMMIT;  -- sent, but takes a while
   ```
   Invoice holds the shared barrier until its commit completes.
2. **B takes the advisory lock** (B's lock session, autocommit):
   ```sql
   SELECT pg_try_advisory_lock(:id);  -- true, A no longer holds it
   ```
   B's lock session is free again at once.
3. **B takes over** (B's pooled connection):
   ```sql
   BEGIN; SET LOCAL lock_timeout = '10s';
   SELECT pg_advisory_xact_lock(ns, bk);  -- exclusive: waits for Invoice
   ```
4. **Stock checks before its commit**:
   ```sql
   SELECT pg_advisory_xact_lock_shared(ns, bk);  -- waits
   ```
   Its shared lock is compatible with Invoice's, but PostgreSQL does not let it jump the queue
   ahead of B's waiting exclusive request. At this point:

   | Lock                | Granted                       | Waiting                                 |
   |---------------------|-------------------------------|-----------------------------------------|
   | key lock `(id)`     | B's lock session, exclusive   | —                                       |
   | barrier `(ns, bk)`  | Invoice, shared               | 1. B's pooled connection, exclusive<br>2. Stock, shared |

5. **Invoice's commit completes.** B gets the barrier and finishes its takeover:
   ```sql
   UPDATE advisory_lock SET generation = 6, owner = 'B' WHERE id = :id;
   -- B's lock session confirms it is still alive (SELECT 1)
   COMMIT;  -- releases the barrier
   ```
   B starts its work.
6. **Stock gets the shared barrier**, reads generation 6 and rolls back with
   `AdvisoryLockFencedException`. Its `withLock` returns `Lost`.

Invoice's writes land before B's work; none of Stock's land at all.

If Stock had checked between steps 2 and 3, it would have got the shared barrier and read
generation 5. That is fine: it holds the barrier, so B waits for it in step 3 as well. This is why
the barrier is taken *before* the generation is read.

Had Invoice run under `REPEATABLE READ`, its check would also hold a `FOR SHARE` row lock until
commit, and B's `UPDATE` in step 5 would wait for it too: on the pooled connection, under the
same `lock_timeout`. If Invoice does not commit in time, B rolls back, releases the advisory lock
and gets `Busy`.

### Why a barrier, and why on a pooled connection

All work on the lock session is serialized: acquisitions and releases of every key and the
heartbeat go through it. So the lock session never waits for another holder. It only takes and
releases advisory locks in autocommit; every wait happens on a pooled connection, bounded by
`barrier-timeout`.

Under `READ COMMITTED`, reading the generation `FOR SHARE` would make the new holder wait just as
well, but a row lock writes to the row (WAL, MultiXacts for concurrent sharers), and the check runs
before every commit. The shared advisory barrier lives only in memory. Under `REPEATABLE READ` and
`SERIALIZABLE` the row lock is needed anyway to see past the snapshot.

## Keys

- `KeyMapper.registry()` (default) maps a key through its text form to the id of its lock row.
  Keys never collide.
- `KeyMapper.direct(key -> id, key -> text)` uses your own advisory lock id. Keys that collide
  only contend with each other: `HeldLocally` within one instance, `Busy` across instances;
  safety is not affected.

## Integration points

| What                         | Use it for                                                              |
|------------------------------|-------------------------------------------------------------------------|
| `AdvisoryLockListener`       | Acquired, failed, released, lost, connection lost and restored.         |
| `LockRecordWriter`           | Writing extra columns of the lock row in the takeover transaction.      |
| `LockHandle.current()`       | The handle from the Reactor context of the work.                        |
| `PgAdvisory.fence(connection)` | A manual fencing check, e.g. without Spring.                          |

## Troubleshooting

| Error                                             | Fix                                                                       |
|---------------------------------------------------|---------------------------------------------------------------------------|
| `Advisory lock table [...] does not exist`        | Set `schema.auto-create=true` or run `V1__advisory_lock.sql`.             |
| `AdvisoryLockFencedException` on commit           | The lock was lost and taken over; the transaction was rolled back on purpose. |
| `LockOutcome.Unavailable` with `LockSessionUnavailableException` | The lock session is down and reconnecting; retry later.    |
| Locks disappear after a while                     | The lock session comes from a pool; point it at an unpooled factory.      |

## Limitations

- Fencing covers database writes in transactions run by `FencingR2dbcTransactionManager`, or
  checked with `fence`. Read-only transactions, autocommit statements and side effects outside
  the database are not fenced.
- Acquiring a lock needs one pooled connection for a short takeover transaction.
- The lock session must not come from a pool. Pool eviction or max life time would close it.
- The lock session must reach PostgreSQL directly or through a pooler in session mode. A pooler in
  transaction mode moves it between backends, and `DISCARD ALL` or `pg_advisory_unlock_all()` on it
  would drop the locks unnoticed.
- `idle_session_timeout` of the database must be longer than `heartbeat.interval`.
- R2DBC only for now. A JDBC variant and short transactional locks (`pg_advisory_xact_lock`)
  are on the roadmap.

## Build

```shell
mvn verify      # needs Docker for Testcontainers
```

See [DEPLOYMENT.md](DEPLOYMENT.md) for releasing to Maven Central.

## License

[Apache 2.0](LICENSE)
