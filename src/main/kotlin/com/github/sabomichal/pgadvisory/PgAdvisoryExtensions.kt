package com.github.sabomichal.pgadvisory

import com.github.sabomichal.pgadvisory.PgAdvisory.FenceCheck
import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome
import com.github.sabomichal.pgadvisory.PgAdvisory.LostLockPolicy
import io.r2dbc.spi.Connection
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.reactor.mono

/**
 * Coroutine variant of [PgAdvisory.withLock]: runs [work] while the lock of [key] is held. Without
 * [lostLockPolicy], the default policy of this instance applies.
 *
 * The work runs in the caller's coroutine context. Cancelling the caller cancels the work and releases the lock.
 * The [LockHandle] travels in the Reactor context, so transactions started inside the work, e.g. with
 * `TransactionalOperator.executeAndAwait`, are fenced by `FencingR2dbcTransactionManager`.
 */
suspend fun <K, T> PgAdvisory<K>.withLockAndAwait(
    key: K,
    lostLockPolicy: LostLockPolicy? = null,
    work: suspend LockHandle.() -> T,
): LockOutcome<T> {
    val context = currentCoroutineContext().minusKey(Job)
    val reactiveWork = { handle: LockHandle -> mono(context) { handle.work() } }
    val outcome = if (lostLockPolicy == null) withLock(key, reactiveWork) else withLock(key, lostLockPolicy, reactiveWork)
    return outcome.awaitSingle()
}

/** Coroutine variant of [PgAdvisory.start]. */
suspend fun PgAdvisory<*>.startAndAwait() {
    start().awaitSingleOrNull()
}

/** Coroutine variant of [PgAdvisory.stop]. */
suspend fun PgAdvisory<*>.stopAndAwait() {
    stop().awaitSingleOrNull()
}

/** Coroutine variant of [PgAdvisory.fence]. */
suspend fun PgAdvisory<*>.fenceAndAwait(connection: Connection): FenceCheck = fence(connection).awaitSingle()

/** Coroutine variant of [LockHandle.verify]. */
suspend fun LockHandle.verifyAndAwait(connection: Connection): FenceCheck = verify(connection).awaitSingle()

/** The handle of the enclosing [withLockAndAwait] or [PgAdvisory.withLock], `null` outside of it. */
suspend fun currentLockHandle(): LockHandle? = LockHandle.current().awaitSingleOrNull()
