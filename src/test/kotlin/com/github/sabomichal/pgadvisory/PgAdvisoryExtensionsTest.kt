package com.github.sabomichal.pgadvisory

import com.github.sabomichal.pgadvisory.PgAdvisory.LockOutcome
import com.github.sabomichal.pgadvisory.PgAdvisory.LostLockPolicy
import com.github.sabomichal.pgadvisory.r2dbc.R2dbcPgAdvisory
import com.github.sabomichal.pgadvisory.spring.FencingR2dbcTransactionManager
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.core.awaitOne
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.time.Duration
import java.util.UUID

class PgAdvisoryExtensionsTest {

    private val instances = mutableListOf<PgAdvisory<String>>()

    @AfterEach
    fun stopInstances() = runBlocking<Unit> {
        instances.forEach { it.stopAndAwait() }
    }

    @Test
    fun `runs suspending work under the lock`() = runBlocking<Unit> {
        val a = advisory("KA")

        val outcome = a.withLockAndAwait(newKey()) {
            delay(10)
            "done"
        }

        assertThat(outcome).isEqualTo(LockOutcome.Completed("done", 1L))
    }

    @Test
    fun `the handle is in the context of the work`() = runBlocking<Unit> {
        val a = advisory("KA")

        val outcome = a.withLockAndAwait(newKey()) { currentLockHandle() === this }

        assertThat(outcome).isEqualTo(LockOutcome.Completed(true, 1L))
        assertThat(currentLockHandle()).isNull()
    }

    @Test
    fun `cancelling the caller releases the lock`() = runBlocking<Unit> {
        val a = advisory("KA")
        val b = advisory("KB")
        val key = newKey()
        val acquired = CompletableDeferred<Unit>()

        val holder = launch {
            a.withLockAndAwait(key) {
                acquired.complete(Unit)
                awaitCancellation()
            }
        }
        acquired.await()
        assertThat(b.withLockAndAwait(key) { "never" }).isEqualTo(LockOutcome.Busy<String>("KA"))
        holder.cancelAndJoin()

        withTimeout(10_000) {
            while (b.withLockAndAwait(key) { "B" } !is LockOutcome.Completed) {
                delay(50)
            }
        }
    }

    @Test
    fun `transactions in the work are fenced`() = runBlocking<Unit> {
        // A does not notice the killed session by itself, so its transaction runs into fencing
        val a = advisory("KA") { it.lostLockPolicy(LostLockPolicy.NOTIFY).heartbeatInterval(Duration.ofMinutes(1)) }
        val b = advisory("KB")
        val key = newKey()
        val pool = TestPostgres.pool()
        val transactional = TransactionalOperator.create(FencingR2dbcTransactionManager(pool))
        val acquired = CompletableDeferred<Unit>()
        val proceed = CompletableDeferred<Unit>()

        val stale = async {
            a.withLockAndAwait(key) {
                acquired.complete(Unit)
                proceed.await()
                transactional.executeAndAwait { DatabaseClient.create(pool).sql("SELECT 1").map { row -> row.get(0) }.awaitOne() }
            }
        }
        acquired.await()
        TestPostgres.terminateLockSession("KA")
        assertThat(b.withLockAndAwait(key) { "B" }).isEqualTo(LockOutcome.Completed("B", 2L))
        proceed.complete(Unit)

        assertThat(stale.await()).isEqualTo(LockOutcome.Lost<Any>(1L))
        pool.dispose()
    }

    private suspend fun advisory(
        owner: String,
        settings: (PgAdvisorySettings.Builder) -> PgAdvisorySettings.Builder = { it },
    ): PgAdvisory<String> {
        val advisory = R2dbcPgAdvisory.builder<String>(TestPostgres.connectionFactory())
            .settings(settings(PgAdvisorySettings.builder().owner(owner).autoCreateSchema(true)).build())
            .build()
        advisory.startAndAwait()
        instances += advisory
        return advisory
    }

    private fun newKey() = UUID.randomUUID().toString()
}
