package com.kofiska.solana.orchestrator

import com.kofiska.solana.orchestrator.domain._
import com.kofiska.solana.orchestrator.ports.{AuditPublisher, DecisionRepository, DedupeCache}
import com.kofiska.solana.orchestrator.service.{ReconciliationService, ReconciliationReport}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

final class ReconciliationServiceSpec extends AsyncFlatSpec with Matchers {
  implicit override def executionContext: ExecutionContext = ExecutionContext.global

  it should "ship later outbox rows even when one row fails" in {
    val repo = new InMemoryRepo
    val cache = new InMemoryCache
    val audit = new FlakyAuditPublisher
    val service = new ReconciliationService(repo, cache, audit, dedupeTtlSeconds = 3600, auditMaxAttempts = 3, auditRetryDelaySeconds = 30)

    service.runOnce(10).map { report =>
      report.pendingAuditCount shouldBe 2
      report.republishedAuditCount shouldBe 1
      report.auditPublishFailureCount shouldBe 1
      report.auditRetryScheduledCount shouldBe 1
      report.auditDeadLetterCount shouldBe 0
      audit.events.map(_.requestId).toVector shouldBe Vector("req-2")
    }
  }

  private final class InMemoryRepo extends DecisionRepository {
    private val first = transitionEvent("req-1", "decision-1", "route-a")
    private val second = transitionEvent("req-2", "decision-2", "route-b")
    private val pending = Vector(first, second)
    private val statuses = TrieMap("req-1" -> "pending", "req-2" -> "pending")

    override def find(requestId: String): Future[Option[DecisionResult]] =
      Future.successful(None)

    override def findByDedupeKey(dedupeKey: String): Future[Option[DecisionResult]] =
      Future.successful(None)

    override def upsert(ctx: RequestContext, result: DecisionResult, event: TransitionEvent): Future[DecisionResult] =
      Future.successful(result)

    override def pendingAudit(limit: Int): Future[Vector[TransitionEvent]] =
      Future.successful(pending.filter(event => statuses.getOrElse(event.requestId, "dead") != "dead").take(limit))

    override def auditBacklogSnapshot(limit: Int): Future[AuditBacklogSnapshot] =
      Future.successful(AuditBacklogSnapshot(
        pendingCount = pending.size.toLong,
        oldestAgeMs = 0L
      ))

    override def connectionPoolSnapshot(): PoolSnapshot =
      PoolSnapshot(0, 0, 0, 0)

    override def markAuditPublished(requestId: String, decisionId: String): Future[Unit] =
      Future.successful(statuses.update(requestId, "sent"))

    override def markAuditFailed(
      requestId: String,
      decisionId: String,
      reason: String,
      maxAttempts: Int,
      retryDelaySeconds: Long
    ): Future[OutboxDeliveryResult] =
      Future.successful {
        statuses.update(requestId, "retry")
        OutboxDeliveryResult.ScheduledRetry
      }

    private def transitionEvent(requestId: String, decisionId: String, routeId: String): TransitionEvent =
      TransitionEvent(
        schemaVersion = "v1",
        traceId = s"trace-$requestId",
        requestId = requestId,
        decisionId = decisionId,
        terminalState = "ACCEPT",
        reasonCode = "ACCEPTED",
        modelVersion = "v1",
        routeId = Some(routeId),
        slot = 100,
        quoteAge = 1,
        sourceHashes = Vector("hash-a", "hash-b"),
        stage = "terminal",
        latencyMs = 1L,
        bytesIn = 10L,
        bytesOut = 10L,
        success = true
      )
  }

  private final class InMemoryCache extends DedupeCache {
    override def get(requestId: String): Future[Option[String]] = Future.successful(None)
    override def claim(requestId: String, marker: String, ttlSeconds: Long): Future[Boolean] = Future.successful(true)
    override def put(requestId: String, decisionId: String, ttlSeconds: Long): Future[Unit] = Future.successful(())
    override def delete(requestId: String): Future[Unit] = Future.successful(())
    override def scan(prefix: String, limit: Int): Future[Vector[String]] = Future.successful(Vector.empty)
  }

  private final class FlakyAuditPublisher extends AuditPublisher {
    val events = scala.collection.mutable.ArrayBuffer.empty[TransitionEvent]

    override def publish(event: TransitionEvent): Future[Unit] =
      if (event.requestId == "req-1") Future.failed(new RuntimeException("poison"))
      else Future.successful {
        events += event
        ()
      }
  }
}
