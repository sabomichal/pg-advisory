# Roadmap

The implementation started as a close port of a proven in-house `LockManager`. A complete implementation
that addresses its weaknesses exists on the branch [`full-implementation`](../../tree/full-implementation)
(commit `66ca6f7`), reviewed twice. It is hard to read as a whole, so it comes back in small steps, each
with its regression tests from that branch. Done steps are removed from here, the git history keeps them.

Every step below names the problem in the current code, the solution on `full-implementation` and the
tests that prove it. The order follows the risk: fencing first, because it protects the data, then the
lost lock connection, then lifecycle and convenience.

The outcomes differ from `full-implementation` on purpose: sealed variants are only the cases the caller
reacts to differently, an error is a typed exception and not an outcome.

## 1. Fencing

**Problem.** After the lock connection died, another instance can take over the key while work of the
old holder is still running. `LockHandle.fencingToken()` lets the target of a write reject the old
holder, but only when the application passes the token along. Nothing checks it before a commit of the
work: both write.

**Solution.** Every work transaction checks its token right before `COMMIT` under a shared advisory
barrier lock, and a new holder takes the barrier exclusively, so it waits for in-flight commits of the
previous holder. `FencingR2dbcTransactionManager` does the check for Spring transactions,
`LockHandle.verify(connection)` by hand. Under `REPEATABLE READ`/`SERIALIZABLE` the token is read
`FOR SHARE`, read-only transactions are not checked.
*Classes:* `Fencer`, `GenerationFencer`, `FencingR2dbcTransactionManager`.
*Tests:* `staleHolderIsFencedAfterTakeover`, `takeoverWaitsForAnInFlightCommitOfThePreviousHolder`,
`barrierTimeoutReturnsBusyAndReleasesTheLock`, `repeatableReadTransactionWithAStaleSnapshotIsFenced`,
`readOnlyTransactionsAreNotFenced`, `transactionInsideTheLockCommits`.

Here it differs on purpose:
- The takeover never waits. On the lock connection the barrier is only tried, and a barrier held by a
  committing transaction of the previous holder unlocks the key again and returns
  `Busy(sameInstance = false)`, like a key held by another instance. The lock connection never blocks,
  so the library needs no pool of its own, unlike `full-implementation`, which moved the takeover to a
  pooled connection. The price: while stale work of the previous holder keeps committing, the key stays
  busy. Its commits are still valid then, no newer token exists yet.
- A fenced commit fails with the typed error `LockFencedException`, which `tryWithLock` passes
  unchanged. No outcome `Lost`.

It comes back in the iterations below, one at a time.

### 1.1 The check before commit, with the barrier (`READ COMMITTED`)

`LockHandle.verify(connection)` runs in the work transaction right before its commit. It takes the
barrier `pg_advisory_xact_lock_shared(namespace, key.hashCode())`, held until the commit, then reads
`fencing_token`, and another token fails with `LockFencedException`. Outside a transaction, and under
another isolation than `READ COMMITTED` until 1.2, it fails. The increment of the fencing token tries
the barrier in the same autocommit statement, `pg_try_advisory_xact_lock(namespace, key.hashCode())`, so
the barrier is held until the new token is committed. A barrier held by the previous holder unlocks the
key again and returns `Busy(sameInstance = false)`. So every commit of the old holder either lands
before the new token or sees it. The namespace of the barrier is a new setting, a `hashCode` collision
only makes a takeover busy more often. *Tests:* `staleHolderIsFencedAfterTakeover`,
`takeoverDuringACommitOfThePreviousHolderIsBusy` (other keys of the new holder do not wait),
`transactionInsideTheLockCommits`.

### 1.2 `REPEATABLE READ` and `SERIALIZABLE`

The snapshot may predate a takeover, so `fencing_token` is read `FOR SHARE`, and a serialization failure
(`40001`) means fenced. The `UPDATE` of a takeover does not wait for that row lock: it is taken only
under the shared barrier, which the try ruled out. A short `lock_timeout` as a safeguard.
*Test:* `repeatableReadTransactionWithAStaleSnapshotIsFenced`.

### 1.3 Spring: `FencingR2dbcTransactionManager`

Calls `verify` in `prepareForCommit` for new transactions inside `tryWithLock` that are not read-only.
To decide: whether the auto-configuration registers it. *Tests:* a fenced commit through
`TransactionalOperator`, `readOnlyTransactionsAreNotFenced`.

## 2. A lost lock connection

**Problem.** The heartbeat notices a lost lock connection within `heartbeat-interval` and replaces its
`LockSession`, but the rest does not cope with the loss. The reset clears the in-memory entries while
old work still runs, so the same instance can lock a key again next to it. The work is not told that
its lock is gone, only the listener gets `onConnectionReset`. A failed unlock leaves its key busy until
a reset, and a failed connect is retried for some errors only, each attempt reported as a reset.

**Solution.** Detection of broken sessions from errors and reconnect with backoff. Running work is told
about the loss: `LostLockPolicy.CANCEL_WORK` cancels it, `NOTIFY` lets it run. Local registrations stay
until the work ends. A new outcome `Lost` and `LockHandle.lost()` tell the caller and the work.
*Classes:* `R2dbcLockSession`, `R2dbcPgAdvisory` (`connectionLost`, `startReconnect`).
*Tests:* `lostSessionCancelsTheWorkAndReconnects`.

It comes back in the iterations below, one at a time.

### 2.1 Per-key unlock recovery

**Problem.** A failed unlock may leave an advisory lock held indefinitely. Resetting the connection
releases it, but also loses every other lock on the same session.

**Solution.** Retry failed unlocks indefinitely on transient errors, scheduling each attempt at the end
of the `SerialExecutor` queue so other lock operations can proceed. Keep the key registered until the
unlock succeeds. Non-transient connection failures trigger a session reset. A repeat is safe: the
registration keeps the key held at most once on the connection, so it returns `true` (released now)
or `false` (released by the failed attempt).

The pending retries belong to the `LockSession` of their connection. A reset replaces the entire
session, so a retry never reaches a new connection, and the finished session discards them: their
locks went with its connection, so their keys are released. The failed unlock after a failed fencing
token resets the session today, it goes the same way. It comes before 2.2, which removes the clearing
that rescues a failed unlock today.
*Tests:* `transientUnlockFailureRetriesWithoutBlockingOtherLocks`, `failedUnlockPreservesOtherLocks`,
`sessionResetDiscardsPendingUnlockRetries`.

### 2.2 A reset keeps the registrations of running work

**Problem.** `replaceSession()` clears `currentlyProcessingLocks` while old work still runs. The same
instance locks the key again next to it, and the old work's release then removes the new work's
registration, so a third lock of the key reenters the new work's lock on the same connection.

**Solution.** No clearing, a registration ends with its work. The old work's unlock ends with its
finished session (the existing warning), nobody else on this instance could have locked the key
meanwhile.
*Test:* `resetKeepsTheKeyOfRunningWorkBusy`: A holds the key, its connection is terminated and reset, a
second lock on A is `Busy(sameInstance = true)`, and after the old work both instances lock the key.

### 2.3 A failed connect is kept only briefly

**Problem.** `SingleConnectionFactory` keeps a failed connect in `Mono.cache()` for good. Only the
heartbeat replaces it, through `checkAndResetConnection`, and only for
`R2dbcNonTransientResourceException`: a refused connect is retried every `heartbeat-interval`, but a
failed login (`28P01`, `R2dbcPermissionDeniedException`) stays even after the password is fixed. And
every failed attempt is reported as a reset: `onConnectionReset`, the reset metric and the warning that
all locks are lost.

**Solution.** The error of a connect is kept for `reconnect-delay` (a new setting, about 1 s), with
`Mono.cache(ttlForValue, ttlForError, ttlForEmpty)`: locks fail fast in between, then the next operation
connects again. `checkAndResetConnection` resets only a connection that existed.
*Test:* a `ConnectionFactory` that refuses the first connects with `R2dbcPermissionDeniedException`, a
later lock succeeds and no reset is reported.

### 2.4 `LockHandle.lost()`

**Problem.** Work under a lock cannot learn that the lock is gone. Only the listener sees
`onConnectionReset`, and not which work it hits.

**Solution.** `currentlyProcessingLocks` maps the key to its handle, a reset marks the handles of all
held locks as lost, `LockHandle.isLost()` and `Mono<Void> lost()` show it. Builds on 2.2.
*Test:* a held key, the connection terminated, `handle.lost()` completes within the heartbeat interval.

### 2.5 Outcome `Lost`

Work that finishes after its lock was lost returns `Lost` instead of `Completed`, the caller has to react
differently. To decide: whether `Lost` carries the value, and what empty work returns.

### 2.6 `LostLockPolicy`

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
- `Mono.usingWhen` wraps an error of the unlock. After completed work it fails with
  `RuntimeException("Async resource cleanup failed after onComplete")` and the value is lost. After
  failed work the work's error is only a suppressed exception of
  `RuntimeException("Async resource cleanup failed after onError")`. The README and `tryWithLock`
  promise both unchanged. To decide: the work's error always unchanged, and whether a failed unlock
  fails `tryWithLock` at all (`full-implementation` only logs it).

## 3. Lifecycle

**Problem.** The constructor starts the heartbeat, the lock manager connects lazily, and `close()`
closes the connection after the queued operations: running work loses its locks in the middle.

**Solution.** `start()`/`stop()`: start connects and starts the heartbeat, stop rejects new locks,
waits for running work up to a shutdown timeout, cancels the rest and only then closes the lock
connection. Both run to their end even if the caller cancels (the caller only stops waiting), and a
stop racing with a detected loss still stops. State changes that the caller must see belong in
`doOnTerminate`, before the terminal signal, otherwise `stop().then(start())` fails on `STOPPING`.
Spring drives it through `SmartLifecycle`.
*Tests:* `stopDrainsRunningWorkAndStartWorksAgain`, `stopCancelsWorkAfterTheShutdownTimeout`,
`cancelledStopStillDrainsAndClosesTheLockSession`, `cancelledStartStillConnects`, `startRightAfterStop`.

## 4. Keys and the lock record

**Solution.** `KeyMapper` (sealed): `registry()` maps the key through its text to the row id, as now,
`direct()` uses an own lock id, with `localKey` so that colliding keys contend within one instance too
(`Busy(sameInstance = true)`). An `owner` column, `Busy.owner`, a `LockRecordWriter` for extra columns
and a lookup of the holder of a key (in the original code the lock record held an RPC endpoint for
routing requests to the holder).
*Tests:* `directKeyMapperCollisionOnlyContends`, `directKeyMapperCollisionIsHeldLocallyWithinOneInstance`.

The `LockRecordWriter` brings a transaction after the lock back (locking itself runs without a
transaction). The lock is released after the rollback of a failed transaction, and a failed unlock
goes as in 2.1. *Test:* `failedRecordWriterReleasesTheLock`.

## 5. Observability, the rest

Depends on step 2.

`LockManagerListener` callbacks `onLost` and `onConnectionRestored`, and a health indicator that reports
the state of the lock connection from the heartbeat, without side effects. A health indicator on
`checkAndResetConnection` would re-create the connection and stop running work on every probe, and compete
with locking for the lock connection.

## 6. Documentation

How fencing works with an example of a takeover, why a barrier and not `FOR SHARE`, and why the
takeover does not wait.
