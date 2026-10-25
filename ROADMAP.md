# Roadmap

The implementation started as a close port of a proven in-house `LockManager`. A complete implementation
that addresses its weaknesses exists on the branch [`full-implementation`](../../tree/full-implementation)
(commit `66ca6f7`), reviewed twice. It is hard to read as a whole, so it comes back in small steps, each
with its regression tests from that branch. Done steps are removed from here, the git history keeps them.

Every step below names the problem in the current code, the solution on `full-implementation` and the
tests that prove it. The order follows the risk: the lost lock connection first, then lifecycle and
convenience.

The outcomes differ from `full-implementation` on purpose: sealed variants are only the cases the caller
reacts to differently, an error is a typed exception and not an outcome. Fencing differs on purpose too:
the library only issues the token, and the target of a write checks it, see
[Fencing token](README.md#fencing-token). No check before commit, no advisory barrier between a takeover
and the work transactions of the previous holder, no `FencingR2dbcTransactionManager`: the library does
not coordinate a takeover with work transactions, and a check of its own would protect only writes to
the same database.

## 1. A lost lock connection

**Problem.** The heartbeat notices a lost lock connection within `heartbeat-interval` and replaces its
`LockSession`, but the rest does not cope with the loss. The reset clears the in-memory entries while
old work still runs, so the same instance can lock a key again next to it. The work is not told that
its lock is gone, only the listener gets `onConnectionReset`. A failed connect is retried for some
errors only, each attempt reported as a reset.

**Solution.** Detection of broken sessions from errors and reconnect with backoff. Running work is told
about the loss: `LostLockPolicy.CANCEL_WORK` cancels it, `NOTIFY` lets it run. Local registrations stay
until the work ends. A new outcome `Lost` and `LockHandle.lost()` tell the caller and the work. They do
not replace fencing, the work may be past its last check when the lock is lost.
*Classes:* `R2dbcLockSession`, `R2dbcPgAdvisory` (`connectionLost`, `startReconnect`).
*Tests:* `lostSessionCancelsTheWorkAndReconnects`.

It comes back in the iterations below, one at a time.

### 1.1 A reset keeps the registrations of running work

**Problem.** `replaceSession()` clears `currentlyProcessingLocks` while old work still runs. The same
instance locks the key again next to it, and the old work's release then removes the new work's
registration, so a third lock of the key reenters the new work's lock on the same connection.

**Solution.** No clearing, a registration ends with its work. The old work's unlock ends with its
finished session (the existing warning), nobody else on this instance could have locked the key
meanwhile.
*Test:* `resetKeepsTheKeyOfRunningWorkBusy`: A holds the key, its connection is terminated and reset, a
second lock on A is `Busy(sameInstance = true)`, and after the old work both instances lock the key.

### 1.2 A failed connect is kept only briefly

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

### 1.3 `LockHandle.lost()`

**Problem.** Work under a lock cannot learn that the lock is gone. Only the listener sees
`onConnectionReset`, and not which work it hits.

**Solution.** `currentlyProcessingLocks` maps the key to its handle, a reset marks the handles of all
held locks as lost, `LockHandle.isLost()` and `Mono<Void> lost()` show it. Builds on 1.1.
*Test:* a held key, the connection terminated, `handle.lost()` completes within the heartbeat interval.

### 1.4 Outcome `Lost`

Work that finishes after its lock was lost returns `Lost` instead of `Completed`, the caller has to react
differently. To decide: whether `Lost` carries the value, and what empty work returns.

### 1.5 `LostLockPolicy`

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
- An unlock that keeps failing on a healthy connection, e.g. after the lock table was dropped, is
  repeated for good, and its `tryWithLock` does not complete until a reset ends the session.

## 2. Lifecycle

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

## 3. Keys and the lock record

**Solution.** `KeyMapper` (sealed): `registry()` maps the key through its text to the row id, as now,
`direct()` uses an own lock id, with `localKey` so that colliding keys contend within one instance too
(`Busy(sameInstance = true)`). An `owner` column, `Busy.owner`, a `LockRecordWriter` for extra columns
and a lookup of the holder of a key (in the original code the lock record held an RPC endpoint for
routing requests to the holder).
*Tests:* `directKeyMapperCollisionOnlyContends`, `directKeyMapperCollisionIsHeldLocallyWithinOneInstance`.

The `LockRecordWriter` brings a transaction after the lock back (locking itself runs without a
transaction). The lock is released after the rollback of a failed transaction, like any other lock.
*Test:* `failedRecordWriterReleasesTheLock`.

## 4. Observability, the rest

Depends on step 1.

`LockManagerListener` callbacks `onLost` and `onConnectionRestored`, and a health indicator that reports
the state of the lock connection from the heartbeat, without side effects. A health indicator on
`checkAndResetConnection` would re-create the connection and stop running work on every probe, and compete
with locking for the lock connection.