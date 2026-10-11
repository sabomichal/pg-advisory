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
connection. When the caller cancels in between (a timeout, a disconnected client), the chain stops:
`onErrorResume` (rollback) does not run on cancel, so the shared connection can stay inside an open
transaction, the session advisory lock may stay held, and `doOnError` does not remove the in-memory
entry, so the key reports `sameInstance` forever. `usingWhen` never got the resource, so it unlocks
nothing.

**Solution.** The lock is the resource of `Mono.usingWhen`; a result that arrives after the caller
left is handed to a callback that releases it (`SerialExecutor.submit(task, onAbandoned)`, which
relies on `MonoSink.onCancel` firing only when the cancel wins over the delivery). The in-memory entry
is removed only after the unlock. *Classes:* `R2dbcPgAdvisory.withLock/release`, `SerialExecutor`.
*Tests:* `cancelWhileLockingOnTheSessionReleasesTheLock`, `cancelWhileTakingOverReleasesTheLock`.

## 2. An error after `pg_try_advisory_lock` leaks the lock

**Problem.** A session advisory lock survives the rollback of the transaction it was taken in. When
anything fails after `pg_try_advisory_lock` succeeded (e.g. the `COMMIT`), the rollback keeps the lock
and `doOnError` removes the in-memory entry. The next `tryWithLock` of the key on this instance gets
the lock a second time (reentrant) and its single unlock leaves it held.

**Solution.** Take the advisory lock in autocommit, outside of any transaction, and release it after
a failed transaction has rolled back. If the unlock fails too, discard the lock connection.
*Tests:* `failedRecordWriterReleasesTheLock`.

## 3. Completion order of cleanups

**Problem.** The mutex of the lock connection is released in `doFinally`, i.e. after the result was
delivered: an operation started right after the completion finds it busy and waits a backoff. On
`full-implementation` the same pattern made a lock taken right after the release fail with
`HeldLocally`, and `stop().then(start())` fail on `STOPPING`.

**Solution.** State changes that the caller must see belong in `doOnTerminate`, before the terminal
signal. *Tests:* `theSameKeyCanBeLockedRightAfterTheRelease`, `startRightAfterStop`.

## 4. The lock connection is a polled mutex

**Problem.** Every operation polls `StampedLock.tryLock()` with backoff. Under load it gives up after
`retry.max-attempts` with `IllegalStateException("LockManager was unable to obtain lock")`, and the
backoff costs throughput: 1 000 concurrent locks take about 30 s in `locksManyKeysInTime`.

**Solution.** A FIFO queue of operations on the lock connection, one at a time, without polling and
without a retry limit: `SerialExecutor` (`Sinks.many().unicast()` + `concatMap`). An operation runs to
completion even if its caller cancels (see step 1). The `retry.*` settings go away.

## 5. A lost lock connection goes unnoticed

**Problem.** Only `validateDbConnection` detects a broken lock connection, called after a
non-transient R2DBC error or by the application. Until then the instance believes it holds its
locks while PostgreSQL has released them. An idle lock connection may even be closed by the server
(`idle_session_timeout`). After a reset the in-memory entries are cleared while old work still runs,
so the same instance can lock a key again next to it, and the old work's unlock hits the new
connection.

**Solution.** A heartbeat (`validate(REMOTE)` with a timeout), detection of broken sessions from
errors, reconnect with backoff, and *incarnations*: every reconnect starts a new one, and unlocks of
an older incarnation are skipped. Running work is told about the loss: `LostLockPolicy.CANCEL_WORK`
cancels it, `NOTIFY` lets it run. Local registrations stay until the work ends.
*Classes:* `R2dbcLockSession`, `R2dbcPgAdvisory` (heartbeat, `connectionLost`, `startReconnect`).
*Tests:* `lostSessionCancelsTheWorkAndReconnects`.

## 6. Fencing

**Problem.** After the lock connection died, another instance can take over the key while work of the
old holder is still running. Nothing stops its commits: both write.

**Solution.** Every acquisition increments the `generation` column of the key's row, the fencing token.
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

## 9. Typed outcomes

**Problem.** A busy key is an exception, mixed with real failures of the work.

**Solution.** `withLock` returns a sealed `LockOutcome`: `Completed`, `Busy(owner)`, `HeldLocally`,
`Lost`, `Unavailable`, so the compiler checks that each is handled. The `LockHandle` is passed to the
work and is in its Reactor context (`LockHandle.current()`).

## 10. Keys and the lock record

**Solution.** `KeyMapper` (sealed): `registry()` maps the key through its text to the row id, as now;
`direct()` uses an own lock id, with `localKey` so that colliding keys contend within one instance too.
An `owner` column, a `LockRecordWriter` for extra columns and a lookup of the holder of a key (in the
original code the lock record held an RPC endpoint for routing requests to the holder).
*Tests:* `directKeyMapperCollisionOnlyContends`, `directKeyMapperCollisionIsHeldLocallyWithinOneInstance`.

## 11. Observability

`AdvisoryLockListener` callbacks (acquired, failed, released, lost, connection lost/restored), Spring
application events (sealed `PgAdvisoryEvents`), Micrometer metrics (`pg.advisory.*`) and a health
indicator that reports the state of the lock connection from the heartbeat (step 5), without side
effects. A health indicator on `validateDbConnection` would re-create the connection and stop running
work on every probe, and compete with locking for the lock connection.

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
