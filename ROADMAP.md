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

**Problem.** Only the validation of the lock connection detects a broken one, run after a
non-transient R2DBC error or by the application. Until then the instance believes it holds its
locks while PostgreSQL has released them. An idle lock connection may even be closed by the server
(`idle_session_timeout`). After a reset the in-memory entries are cleared while old work still runs,
so the same instance can lock a key again next to it, and the old work's unlock hits the new
connection.

A failed connect is never retried. `SingleConnectionFactory` keeps the error in `Mono.cache()`, so every
later `tryWithLock` fails with the same `LockFailedException`. The validation that would re-create the
connection does not start either: it needs `R2dbcNonTransientResourceException` as the root cause, but
a refused connect is `R2dbcNonTransientResourceException <- AnnotatedConnectException <- ConnectException`.
The instance stays unusable until the application validates the connection or restarts.

**Solution.** A heartbeat (`validate(REMOTE)` with a timeout), detection of broken sessions from
errors, reconnect with backoff, and *incarnations*: every reconnect starts a new one, and unlocks of
an older incarnation are skipped. Running work is told about the loss: `LostLockPolicy.CANCEL_WORK`
cancels it, `NOTIFY` lets it run. Local registrations stay until the work ends. A new outcome `Lost`
and `LockHandle.lost()` tell the caller and the work.
*Classes:* `R2dbcLockSession`, `R2dbcPgAdvisory` (heartbeat, `connectionLost`, `startReconnect`).
*Tests:* `lostSessionCancelsTheWorkAndReconnects`.

It comes back in the iterations below, one at a time. 1.2 to 1.7 follow after step 2: fencing protects
the data and matters more.

### 1.1 Heartbeat (done)

Every `heartbeat-interval` (5 s) the heartbeat runs `checkAndResetConnection` through the queue like any
other operation, the next one only after the previous finished. `validate(REMOTE)` gets
`heartbeat-timeout` (5 s): a connection that does not answer counts as broken, otherwise it would hold
up the queue for good. A broken connection is re-created and `onConnectionReset` is called. The public
`validateDbConnection` is gone: the heartbeat does its job, and its name hid the reset.
*Tests:* `heartbeatReconnectsALostLockConnection`, `heartbeatResetsALockConnectionThatDoesNotAnswer`.

The heartbeat also retries a failed connect: `checkAndResetConnection` re-creates the factory that keeps
the error. But only for `R2dbcNonTransientResourceException`, and every failed attempt is reported as a
reset, see 1.4.

### 1.2 A failed unlock resets the lock connection

**Problem.** A failed unlock leaves the key registered and the advisory lock possibly held, until a
reset clears the registrations. A transient error (`statement_timeout`, `lock_timeout`, a cancelled
query) starts no reset, `checkConnectionOnError` reacts to non-transient errors only. The key then stays
`Busy(sameInstance = true)` here and `Busy(sameInstance = false)` elsewhere for good.

**Solution.** A failed unlock resets the lock connection, closing it surely releases the lock, and only
then the registration is removed. The error still reaches the caller. Step 5 discards the connection
after a failed unlock as well. It comes before 1.3, which removes the clearing that rescues a failed
unlock today. *Test:* `failedUnlockResetsTheLockConnection`: the first `pg_advisory_unlock` fails with
`R2dbcTransientResourceException`, then both instances lock the key.

### 1.3 A reset keeps the registrations of running work

**Problem.** `newConnectionFactory()` clears `currentlyProcessingLocks` while old work still runs. The
same instance locks the key again next to it, and the old work's unlock on the new connection releases
the new holder's lock, so the key can be locked a third time.

**Solution.** No clearing, a registration ends with its work. The old work's unlock on the new
connection finds the key not held and returns `false` (the existing warning), nobody else on this
instance could have locked it meanwhile. Incarnations are not needed for that.
*Test:* `resetKeepsTheKeyOfRunningWorkBusy`: A holds the key, its connection is terminated and reset, a
second lock on A is `Busy(sameInstance = true)`, and after the old work both instances lock the key.

### 1.4 A failed connect is kept only briefly

**Problem.** `SingleConnectionFactory` keeps a failed connect in `Mono.cache()` for good. Only
`checkAndResetConnection` replaces it, and only for `R2dbcNonTransientResourceException`: a failed
login (`28P01`, `R2dbcPermissionDeniedException`) stays even after the password is fixed. And every
failed attempt is reported as a reset: `onConnectionReset`, the reset metric and the warning that all
locks are lost.

**Solution.** The error of a connect is kept for `reconnect-delay` (a new setting, about 1 s), with
`Mono.cache(ttlForValue, ttlForError, ttlForEmpty)`: locks fail fast in between, then the next operation
connects again. `checkAndResetConnection` resets only a connection that existed.
*Test:* a `ConnectionFactory` that refuses the first connects with `R2dbcPermissionDeniedException`, a
later lock succeeds and no reset is reported.

### 1.5 `LockHandle.lost()`

**Problem.** Work under a lock cannot learn that the lock is gone. Only the listener sees
`onConnectionReset`, and not which work it hits.

**Solution.** `currentlyProcessingLocks` maps the key to its handle, a reset marks the handles of all
held locks as lost, `LockHandle.isLost()` and `Mono<Void> lost()` show it. Builds on 1.3.
*Test:* a held key, the connection terminated, `handle.lost()` completes within the heartbeat interval.

### 1.6 Outcome `Lost`

Work that finishes after its lock was lost returns `Lost` instead of `Completed`, the caller has to react
differently. To decide: whether `Lost` carries the value, and what empty work returns.

### 1.7 `LostLockPolicy`

`CANCEL_WORK` cancels the work on the loss (`takeUntilOther(handle.lost())`) and returns `Lost`, `NOTIFY`
lets it run. To decide: the default, and a per-call overload or a setting only.

### Open, no iteration yet

- Errors of the locking itself are detected by their root cause only, the solution above looks at the
  whole cause chain. With the heartbeat this only delays the reset by up to `heartbeat-interval`, so it
  waits for a reproducible case.
- An operation stuck on a half-open connection holds up the queue and the heartbeat behind it, only the
  validation has a timeout. A timeout on every operation would also reset while an `INSERT` legitimately
  waits for another client's uncommitted row of the key (see `cancelWhileLockingReleasesTheLock`), so it
  needs a decision of its own.

## 2. Fencing

**Problem.** After the lock connection died, another instance can take over the key while work of the
old holder is still running. Nothing stops its commits: both write.

**Solution.** Every lock acquisition increments the fencing token in the key's row. Every work
transaction checks its token right before `COMMIT` under a shared advisory barrier
lock, and a new holder takes the barrier exclusively, so it waits for in-flight commits of the previous
holder. `FencingR2dbcTransactionManager` does the check for Spring transactions, `fence(connection)`
by hand. Under `REPEATABLE READ`/`SERIALIZABLE` the token is read `FOR SHARE`, read-only
transactions are not checked. The token goes into `Completed` and `LockHandle`, together with
`LockHandle.verify()`. Empty work has no `Completed`, so its token is only in the `LockHandle`.
*Classes:* `Fencer`, `GenerationFencer`, `FencingR2dbcTransactionManager`.
*Tests:* `staleHolderIsFencedAfterTakeover`, `takeoverWaitsForAnInFlightCommitOfThePreviousHolder`,
`barrierTimeoutReturnsBusyAndReleasesTheLock`, `repeatableReadTransactionWithAStaleSnapshotIsFenced`,
`readOnlyTransactionsAreNotFenced`, `transactionInsideTheLockCommits`.

Here it differs on purpose:
- The takeover never waits. On the lock connection the barrier is only tried, and a barrier held by a
  committing transaction of the previous holder unlocks the key again and returns
  `Busy(sameInstance = false)`, like a key held by another instance. The lock connection never blocks,
  so the library needs no pool of its own and step 3 is dropped. The price: while stale work of the
  previous holder keeps committing, the key stays busy. Its commits are still valid then, no newer
  token exists yet.
- A fenced commit fails with the typed error `LockFencedException`, which `tryWithLock` passes
  unchanged. No outcome `Lost`.
- The token is only in the `LockHandle`, `Completed` stays as it is.

It comes back in the iterations below, one at a time.

### 2.1 Fencing token (done)

Every lock increments `fencing_token` on the lock connection right after `pg_try_advisory_lock`, in
autocommit: `UPDATE … SET fencing_token = fencing_token + 1 … RETURNING fencing_token`. The new value is
`LockHandle.fencingToken()`. The session lock survives a failed increment, so the key is unlocked again
on the same connection, and when that fails too, the lock connection is reset. The column is in
`V1__advisory_lock.sql` and in the table that auto-create makes. No migration: nothing was released yet.
A lock cycle takes 4 round trips instead of 3, yet `locksManyKeysInTime` showed no measurable change in
one run each: 4.9 s before, 4.8 s after.
*Tests:* `fencingTokenGrowsWithEveryLockOfTheKey`, `failedFencingTokenReleasesTheLock`.

### 2.2 The check before commit, with the barrier (`READ COMMITTED`)

`LockHandle.verify(connection)` runs in the work transaction right before its commit. It takes the
barrier `pg_advisory_xact_lock_shared(namespace, key.hashCode())`, held until the commit, then reads
`fencing_token`, and another token fails with `LockFencedException`. Outside a transaction, and under
another isolation than `READ COMMITTED` until 2.3, it fails. The increment of 2.1 tries the barrier in
the same autocommit statement, `pg_try_advisory_xact_lock(namespace, key.hashCode())`, so the barrier
is held until the new token is committed. A barrier held by the previous holder unlocks the key again
and returns `Busy(sameInstance = false)`. So every commit of the old holder either lands before the new
token or sees it. The namespace of the barrier is a new setting, a `hashCode` collision only makes a
takeover busy more often. *Tests:* `staleHolderIsFencedAfterTakeover`,
`takeoverDuringACommitOfThePreviousHolderIsBusy` (other keys of the new holder do not wait),
`transactionInsideTheLockCommits`.

### 2.3 `REPEATABLE READ` and `SERIALIZABLE`

The snapshot may predate a takeover, so `fencing_token` is read `FOR SHARE`, and a serialization failure
(`40001`) means fenced. The `UPDATE` of a takeover does not wait for that row lock: it is taken only
under the shared barrier, which the try ruled out. A short `lock_timeout` as a safeguard.
*Test:* `repeatableReadTransactionWithAStaleSnapshotIsFenced`.

### 2.4 Spring: `FencingR2dbcTransactionManager`

Calls `verify` in `prepareForCommit` for new transactions inside `tryWithLock` that are not read-only.
To decide: whether the auto-configuration registers it. *Tests:* a fenced commit through
`TransactionalOperator`, `readOnlyTransactionsAreNotFenced`.

## 3. Takeover without blocking the lock connection (dropped)

On `full-implementation` the takeover waits for in-flight commits of the previous holder, so it had to
move from the serialized lock connection to a pooled one, with a confirmation on the same incarnation
of the lock connection that the lock is still held. Here the takeover never waits (step 2), and a pool
of its own is out of the scope of the library.

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
`checkAndResetConnection` would re-create the connection and stop running work on every probe, and compete
with locking for the lock connection.

## 7. Locking abandoned keys in bulk

The original code locked all abandoned keys of a table in one statement (`pg_try_advisory_lock` over
the rows that nobody holds according to `pg_locks`) and marked them prepared in advance, so that the
following `tryWithLock` reused the lock. Generalized: the application passes the candidate keys.
A public `unlock(key)` comes back with it, for keys locked in bulk whose work could not be started.
Outside of that, `tryWithLock` releases its own locks and a public unlock could only release a lock
under running work.

## 8. Documentation

How fencing works with an example of a takeover, why a barrier and not `FOR SHARE`, why the takeover
does not wait, and the
requirements on the database: no transaction-mode pooler in front of the lock connection,
`idle_session_timeout` longer than the heartbeat interval.
