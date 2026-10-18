# Roadmap

The current implementation is a close port of a proven in-house `LockManager`: simple and readable, but
with known weaknesses. A complete implementation that addresses all of them exists on the branch
[`full-implementation`](../../tree/full-implementation) (commit `66ca6f7`), reviewed twice. It is hard to
read as a whole, so it comes back in small steps, each with its regression tests from that branch.

Every step below names the problem in the current code, the solution on `full-implementation` and the
tests that prove it. The order follows the risk: lock leaks first, then detection of a lost lock,
then fencing, then convenience.

## 1. Cancelling while locking leaks the lock

**Problem.** `lock()` runs `BEGIN`, `INSERT`, `pg_try_advisory_lock`, `COMMIT` on the shared lock
connection. Since step 4 it runs to its end even when the caller cancels in between (a timeout, a
disconnected client), so the connection no longer stays inside an open transaction. But the lock taken
for a caller that left is never released: `usingWhen` never got the resource, so it unlocks nothing,
the session advisory lock stays held and the in-memory entry keeps the key `Busy(sameInstance = true)`
forever.

**Solution.** The lock is the resource of `Mono.usingWhen`; a result that arrives after the caller
left is handed to a callback that releases it (`SerialExecutor.submit(task, onAbandoned)`, which
relies on `MonoSink.onCancel` firing only when the cancel wins over the delivery). The in-memory entry
is removed only after the unlock. *Classes:* `R2dbcPgAdvisory.withLock/release`, `SerialExecutor`.
*Tests:* `cancelWhileLockingOnTheSessionReleasesTheLock`, `cancelWhileTakingOverReleasesTheLock`.

## 2. An error after `pg_try_advisory_lock` leaks the lock (done)

**Problem.** A session advisory lock survives the rollback of the transaction it was taken in. When
anything fails after `pg_try_advisory_lock` succeeded (e.g. the `COMMIT`), the rollback keeps the lock
and `doOnError` removes the in-memory entry. The next `tryWithLock` of the key on this instance gets
the lock a second time (reentrant) and its single unlock leaves it held, and other instances find the
key busy.

**Done.** The advisory lock is taken in autocommit, outside of any transaction, so nothing can fail
after it. The transaction protected nothing, `INSERT … ON CONFLICT DO NOTHING` is atomic on its own.
Releasing the lock after a failed transaction comes back with the `LockRecordWriter` of step 10.
*Test:* `failingCommitDoesNotLeakTheLock`, on a key whose row exists. For a new key the rollback also
removed the row, so the next lock got a new id and the leaked lock stayed hidden.

It also speeds locking up: a lock/unlock cycle drops from 5 round trips (`BEGIN`, `INSERT`,
`pg_try_advisory_lock`, `COMMIT`, `pg_advisory_unlock`) to 3. Since step 4 a cycle costs almost only its
round trips. Measured with `R2dbcLockManagerBenchmark` on Docker Desktop for Windows: 235 / 233 / 237 →
390 / 371 / 383 cycles per second at 1 / 16 / 256 at once, latency of a single caller 4.3 → 2.6 ms
(p50), with 256 callers 1021 → 653 ms. `locksManyKeysInTime` 5.5 s → 3.8 s, less than 5/3 faster,
because its keys are new and the `INSERT` now carries the cost of the commit.

## 3. Completion order of cleanups (done with step 4)

**Problem.** The mutex of the lock connection is released in `doFinally`, i.e. after the result was
delivered: an operation started right after the completion finds it busy and waits a backoff. On
`full-implementation` the same pattern made a lock taken right after the release fail with
`HeldLocally` (here `Busy(sameInstance = true)`), and `stop().then(start())` fail on `STOPPING`.

**Done.** The mutex is gone with step 4, the next operation waits in the queue and starts right after
the previous one. The local entry is removed in `doOnSuccess`, before the result is delivered, so the
`HeldLocally` problem never applied here. *Test:* `theSameKeyCanBeLockedRightAfterTheRelease`.
`startRightAfterStop` comes with the lifecycle of step 8, where state changes belong in `doOnTerminate`.

## 4. The lock connection is a polled mutex (done)

**Problem.** Every operation polls `StampedLock.tryLock()` with backoff. Under load it gives up after
`retry.max-attempts`: locking fails with `LockFailedException`, the other operations with
`IllegalStateException("LockManager was unable to obtain lock")`. And the backoff costs throughput:
1 000 concurrent locks take about 30 s in `locksManyKeysInTime`.

**Done.** A FIFO queue of operations on the lock connection, one at a time, without polling and
without a retry limit: `SerialExecutor` (`Sinks.many().unicast()` + `concatMap`). An operation runs to
its end even if its caller cancels, releasing the lock of a caller that left comes with step 1. The
`retry.*` settings are gone. `close()` waits up to `shutdown-lock-timeout` for the queued operations
and then closes the lock connection anyway, instead of throwing and leaving it open.

Measured with `R2dbcLockManagerBenchmark` on Docker Desktop for Windows: 26 / 30 / 58 → 235 / 233 / 237
lock/unlock cycles per second at 1 / 16 / 256 at once, mean latency of a single caller 31 → 4.5 ms. With
256 callers the old code failed with `unable to obtain lock`, the queue serves each in about 1 s
(256 × 4 ms). `locksManyKeysInTime` 32.5 s → 5.5 s. What is left is the cost of 5 round trips per cycle
on one connection, step 2 removes two of them. *Tests:* `SerialExecutorTest`, `locksManyKeysInTime`
(bound 15 s), `contendedKeysAreNeverHeldTwice`, `theSameKeyCanBeLockedRightAfterTheRelease`.

## 5. A lost lock connection goes unnoticed

**Problem.** Only `validateDbConnection` detects a broken lock connection, called after a
non-transient R2DBC error or by the application. Until then the instance believes it holds its
locks while PostgreSQL has released them. An idle lock connection may even be closed by the server
(`idle_session_timeout`). After a reset the in-memory entries are cleared while old work still runs,
so the same instance can lock a key again next to it, and the old work's unlock hits the new
connection.

A failed connect is never retried. `SingleConnectionFactory` keeps the error in `Mono.cache()`, so every
later `tryWithLock` fails with the same `LockFailedException`. The validation that would re-create the
connection does not start either: it needs `R2dbcNonTransientResourceException` as the root cause, but
a refused connect is `R2dbcNonTransientResourceException <- AnnotatedConnectException <- ConnectException`.
The instance stays unusable until the application calls `validateDbConnection` or restarts.

**Solution.** A heartbeat (`validate(REMOTE)` with a timeout), detection of broken sessions from
errors, reconnect with backoff, and *incarnations*: every reconnect starts a new one, and unlocks of
an older incarnation are skipped. Running work is told about the loss: `LostLockPolicy.CANCEL_WORK`
cancels it, `NOTIFY` lets it run. Local registrations stay until the work ends.
*Classes:* `R2dbcLockSession`, `R2dbcPgAdvisory` (heartbeat, `connectionLost`, `startReconnect`).
*Tests:* `lostSessionCancelsTheWorkAndReconnects`.

This also fixes the failed connect, if the reconnect does not keep a failed attempt and the error
detection looks at the whole cause chain. *Test to add:* a `ConnectionFactory` that refuses the first
connects with `R2dbcNonTransientResourceException(new ConnectException())`, after which a later lock succeeds.

## 6. Fencing

**Problem.** After the lock connection died, another instance can take over the key while work of the
old holder is still running. Nothing stops its commits: both write.

**Solution.** Every lockAcquisition increments the `generation` column of the key's row, the fencing token.
Every work transaction checks its token right before `COMMIT` under a shared advisory barrier lock,
and a new holder takes the barrier exclusively, so it waits for in-flight commits of the previous
holder. `FencingR2dbcTransactionManager` does the check for Spring transactions, `fence(connection)`
by hand. Under `REPEATABLE READ`/`SERIALIZABLE` the generation is read `FOR SHARE`; read-only
transactions are not checked. *Classes:* `Fencer`, `GenerationFencer`, `FencingR2dbcTransactionManager`.
*Tests:* `staleHolderIsFencedAfterTakeover`, `takeoverWaitsForAnInFlightCommitOfThePreviousHolder`,
`barrierTimeoutReturnsBusyAndReleasesTheLock`, `repeatableReadTransactionWithAStaleSnapshotIsFenced`,
`readOnlyTransactionsAreNotFenced`, `transactionInsideTheLockCommits`.

## 7. Takeover without blocking the lock connection

Depends on step 6.

**Problem.** Waiting for the previous holder (the barrier, or a `FOR SHARE` row lock of a
`REPEATABLE READ` transaction) must not happen on the serialized lock connection: it would stall every
key and the heartbeat. And when the token is issued on another connection, the advisory lock may be
lost in between and another holder may already have taken over.

**Solution.** The lock connection only runs `pg_try_advisory_lock`/`pg_advisory_unlock` in autocommit.
The barrier and the new token are one transaction on a pooled connection under
`lock_timeout = barrier-timeout`, and the token is committed only if a round trip on the same
incarnation of the lock connection confirms it is still alive, while the exclusive barrier keeps
every later takeover waiting. *Tests:* `repeatableReadTransactionOfThePreviousHolderDoesNotBlockTheLockSession`,
`lateTakeoverAfterTheLockWasLostIsRejected`, `lateTakeoverAfterANoticedLossIsRejected`,
`lockLostWhileWaitingForTheBarrierIsNotTakenOver`.

## 8. Lifecycle

**Problem.** The lock manager connects lazily and `close()` closes the connection at once: running
work loses its locks in the middle.

**Solution.** `start()`/`stop()`: stop rejects new locks, waits for running work up to a shutdown
timeout, cancels the rest and only then closes the lock connection. Both run to their end even if
the caller cancels (the caller only stops waiting), the state changes before completion, and a stop
racing with a detected loss still stops. Spring drives it through `SmartLifecycle`.
*Tests:* `stopDrainsRunningWorkAndStartWorksAgain`, `stopCancelsWorkAfterTheShutdownTimeout`,
`cancelledStopStillDrainsAndClosesTheLockSession`, `cancelledStartStillConnects`, `startRightAfterStop`.

## 9. Typed outcomes (done)

**Problem.** A busy key was an exception, mixed with real failures of the work.

**Done.** `tryWithLock` returns a sealed `LockManager.LockOutcome`: `Completed(value)` and
`Busy(sameInstance)`. The work and the `onLock`/`onFinally` callbacks get a `LockHandle` with the key,
which is also in the Reactor context of the work (`LockHandle.current()`).
It differs from `full-implementation` on purpose: sealed variants are only the cases the caller reacts to
differently.

- `HeldLocally` is `Busy(sameInstance = true)`, from step 10 on also for colliding `localKey`s.
- `Unavailable` is not an outcome but the error `LockFailedException(cause)`: the key could not be
  locked because of an error, and the work did not run.
- `Completed` never holds `null`. Empty work, e.g. a `Mono<Void>`, makes `tryWithLock` complete empty.

Later steps add `Lost` (step 5), `Busy.owner` (step 10), and the fencing token in `Completed` and
`LockHandle` together with its `lost()` and `verify()` (steps 5 and 6). Empty work has no `Completed`,
so its token is only in the `LockHandle`.

## 10. Keys and the lock record

**Solution.** `KeyMapper` (sealed): `registry()` maps the key through its text to the row id, as now;
`direct()` uses an own lock id, with `localKey` so that colliding keys contend within one instance too.
An `owner` column, a `LockRecordWriter` for extra columns and a lookup of the holder of a key (in the
original code the lock record held an RPC endpoint for routing requests to the holder).
*Tests:* `directKeyMapperCollisionOnlyContends`, `directKeyMapperCollisionIsHeldLocallyWithinOneInstance`.

The `LockRecordWriter` brings a transaction after the lock back (step 2 removed the one around it). The
lock is released after the rollback of a failed transaction, and when the unlock fails too, the lock
connection is discarded. *Test:* `failedRecordWriterReleasesTheLock`.

## 11. Observability (partly done)

**Done.** `LockManagerListener` with `onLocked`, `onBusy`, `onLockFailed`, `onReleased` and
`onConnectionReset`, which replaced `ConnectionResetListener`. Every `onLocked` is followed by exactly one
`onReleased`, also after a failed unlock, so that the gauge of held locks stays consistent. On top of it
Spring application events (sealed `PgAdvisoryEvents`) and Micrometer metrics (`pg.advisory.*`,
`MicrometerLockManagerListener`, registered when there is a `MeterRegistry`). Unlike `full-implementation`, a
failed lock is `onLockFailed` with the `LockFailedException`, not an outcome (see step 9). The
notifications move with steps 1, 4 and 5, the listener stays.

**With step 5.** `onLost`, `onConnectionRestored` and a health indicator that reports the state of the
lock connection from the heartbeat, without side effects. A health indicator on `validateDbConnection`
would re-create the connection and stop running work on every probe, and compete with locking for the
lock connection.

## 12. Locking abandoned keys in bulk

The original code locked all abandoned keys of a table in one statement (`pg_try_advisory_lock` over
the rows that nobody holds according to `pg_locks`) and marked them prepared in advance, so that the
following `tryWithLock` reused the lock. Generalized: the application passes the candidate keys.
A public `unlock(key)` comes back with it, for keys locked in bulk whose work could not be started;
outside of that, `tryWithLock` releases its own locks and a public unlock could only release a lock
under running work.

## 13. Kotlin extensions

Coroutine variants (`withLockAndAwait`, `startAndAwait`, …) with an optional dependency on
`kotlinx-coroutines-reactor`. *Tests:* `PgAdvisoryExtensionsTest` on `full-implementation`.

## 14. Documentation

How fencing works with an example of a takeover, why a barrier and not `FOR SHARE`, and the
requirements on the database: no transaction-mode pooler in front of the lock connection,
`idle_session_timeout` longer than the heartbeat interval.
