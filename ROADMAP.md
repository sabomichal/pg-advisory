# Roadmap

The current implementation is a close port of a proven in-house `LockManager`: simple and readable, but
with known weaknesses. A complete implementation that addresses all of them exists on the branch
[`full-implementation`](../../tree/full-implementation) (commit `66ca6f7`), reviewed twice. It is hard to
read as a whole, so it comes back in small steps, each with its regression tests from that branch.

Every step below names the problem in the current code, the solution on `full-implementation` and the
tests that prove it. The order follows the risk: lock leaks first, then detection of a lost lock,
then fencing, then convenience.

The outcomes differ from `full-implementation` on purpose: sealed variants are only the cases the caller
reacts to differently. `HeldLocally` is `Busy(sameInstance = true)`, and `Unavailable` is not an outcome
but the error `LockFailedException(cause)`: the key could not be locked because of an error, and the work
did not run.

## 1. A lost lock connection goes unnoticed

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
cancels it, `NOTIFY` lets it run. Local registrations stay until the work ends. A new outcome `Lost`
and `LockHandle.lost()` tell the caller and the work.
*Classes:* `R2dbcLockSession`, `R2dbcPgAdvisory` (heartbeat, `connectionLost`, `startReconnect`).
*Tests:* `lostSessionCancelsTheWorkAndReconnects`.

This also fixes the failed connect, if the reconnect does not keep a failed attempt and the error
detection looks at the whole cause chain. *Test to add:* a `ConnectionFactory` that refuses the first
connects with `R2dbcNonTransientResourceException(new ConnectException())`, after which a later lock succeeds.

## 2. Fencing

**Problem.** After the lock connection died, another instance can take over the key while work of the
old holder is still running. Nothing stops its commits: both write.

**Solution.** Every lock acquisition increments the `generation` column of the key's row, the fencing
token. Every work transaction checks its token right before `COMMIT` under a shared advisory barrier
lock, and a new holder takes the barrier exclusively, so it waits for in-flight commits of the previous
holder. `FencingR2dbcTransactionManager` does the check for Spring transactions, `fence(connection)`
by hand. Under `REPEATABLE READ`/`SERIALIZABLE` the generation is read `FOR SHARE`, read-only
transactions are not checked. The token goes into `Completed` and `LockHandle`, together with
`LockHandle.verify()`. Empty work has no `Completed`, so its token is only in the `LockHandle`.
*Classes:* `Fencer`, `GenerationFencer`, `FencingR2dbcTransactionManager`.
*Tests:* `staleHolderIsFencedAfterTakeover`, `takeoverWaitsForAnInFlightCommitOfThePreviousHolder`,
`barrierTimeoutReturnsBusyAndReleasesTheLock`, `repeatableReadTransactionWithAStaleSnapshotIsFenced`,
`readOnlyTransactionsAreNotFenced`, `transactionInsideTheLockCommits`.

## 3. Takeover without blocking the lock connection

Depends on step 2.

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
`lockLostWhileWaitingForTheBarrierIsNotTakenOver`, `cancelWhileTakingOverReleasesTheLock`.

## 4. Lifecycle

**Problem.** The lock manager connects lazily and `close()` closes the connection after the queued
operations: running work loses its locks in the middle.

**Solution.** `start()`/`stop()`: stop rejects new locks, waits for running work up to a shutdown
timeout, cancels the rest and only then closes the lock connection. Both run to their end even if
the caller cancels (the caller only stops waiting), and a stop racing with a detected loss still stops.
State changes that the caller must see belong in `doOnTerminate`, before the terminal signal, otherwise
`stop().then(start())` fails on `STOPPING`. Spring drives it through `SmartLifecycle`.
*Tests:* `stopDrainsRunningWorkAndStartWorksAgain`, `stopCancelsWorkAfterTheShutdownTimeout`,
`cancelledStopStillDrainsAndClosesTheLockSession`, `cancelledStartStillConnects`, `startRightAfterStop`.

## 5. Keys and the lock record

**Solution.** `KeyMapper` (sealed): `registry()` maps the key through its text to the row id, as now,
`direct()` uses an own lock id, with `localKey` so that colliding keys contend within one instance too
(`Busy(sameInstance = true)`). An `owner` column, `Busy.owner`, a `LockRecordWriter` for extra columns
and a lookup of the holder of a key (in the original code the lock record held an RPC endpoint for
routing requests to the holder).
*Tests:* `directKeyMapperCollisionOnlyContends`, `directKeyMapperCollisionIsHeldLocallyWithinOneInstance`.

The `LockRecordWriter` brings a transaction after the lock back (locking itself runs without a
transaction). The lock is released after the rollback of a failed transaction, and when the unlock
fails too, the lock connection is discarded. *Test:* `failedRecordWriterReleasesTheLock`.

## 6. Observability, the rest

Depends on step 1.

`LockManagerListener` callbacks `onLost` and `onConnectionRestored`, and a health indicator that reports
the state of the lock connection from the heartbeat, without side effects. A health indicator on
`validateDbConnection` would re-create the connection and stop running work on every probe, and compete
with locking for the lock connection.

## 7. Locking abandoned keys in bulk

The original code locked all abandoned keys of a table in one statement (`pg_try_advisory_lock` over
the rows that nobody holds according to `pg_locks`) and marked them prepared in advance, so that the
following `tryWithLock` reused the lock. Generalized: the application passes the candidate keys.
A public `unlock(key)` comes back with it, for keys locked in bulk whose work could not be started.
Outside of that, `tryWithLock` releases its own locks and a public unlock could only release a lock
under running work.

## 8. Documentation

How fencing works with an example of a takeover, why a barrier and not `FOR SHARE`, and the
requirements on the database: no transaction-mode pooler in front of the lock connection,
`idle_session_timeout` longer than the heartbeat interval.
