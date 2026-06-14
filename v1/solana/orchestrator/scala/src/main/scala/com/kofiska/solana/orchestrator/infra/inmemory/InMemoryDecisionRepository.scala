package com.kofiska.solana.orchestrator.infra.inmemory

import com.kofiska.solana.orchestrator.domain.{AuditBacklogSnapshot, DecisionResult, OutboxDeliveryResult, PoolSnapshot, RequestContext, TransitionEvent}
import com.kofiska.solana.orchestrator.ports.DecisionRepository

import scala.collection.concurrent.TrieMap
import scala.concurrent.{ExecutionContext, Future}

final class InMemoryDecisionRepository(implicit ec: ExecutionContext) extends DecisionRepository {
  private val rows = TrieMap.empty[String, DecisionResult]
  private val dedupeRows = TrieMap.empty[String, String]
  private val auditRows = TrieMap.empty[(String, String), InMemoryOutboxRow]
  private val published = TrieMap.empty[(String, String), Boolean]

  override def find(requestId: String): Future[Option[DecisionResult]] =
    Future.successful(rows.get(requestId))

  override def findByDedupeKey(dedupeKey: String): Future[Option[DecisionResult]] =
    Future.successful(dedupeRows.get(dedupeKey).flatMap(rows.get))

  override def upsert(ctx: RequestContext, result: DecisionResult, event: TransitionEvent): Future[DecisionResult] =
    Future {
      val requestId = dedupeRows.getOrElseUpdate(ctx.dedupeKey, ctx.requestId)
      val durable = rows.getOrElseUpdate(requestId, result.copy(requestId = requestId))
      auditRows.putIfAbsent((durable.requestId, durable.decisionId), InMemoryOutboxRow(event, "pending", 0, None, None))
      durable
    }

  override def pendingAudit(limit: Int): Future[Vector[TransitionEvent]] =
    Future.successful {
      auditRows.collect {
        case ((requestId, decisionId), row)
            if !published.contains((requestId, decisionId)) && row.status != "dead" && row.nextRetryAt.forall(_ <= System.currentTimeMillis()) => row.event
      }.take(limit).toVector
    }

  override def auditBacklogSnapshot(limit: Int): Future[AuditBacklogSnapshot] =
    Future.successful {
      val now = System.currentTimeMillis()
      val rows = auditRows.collect {
        case (_, row) if row.status != "dead" && row.nextRetryAt.forall(_ <= now) => row
      }.toVector
      AuditBacklogSnapshot(
        pendingCount = rows.size.toLong,
        oldestAgeMs = 0L
      )
    }

  override def connectionPoolSnapshot(): PoolSnapshot =
    PoolSnapshot(0, 0, 0, 0)

  override def markAuditPublished(requestId: String, decisionId: String): Future[Unit] =
    Future.successful {
      published.put((requestId, decisionId), true)
      auditRows.get((requestId, decisionId)).foreach { row =>
        auditRows.put((requestId, decisionId), row.copy(status = "sent"))
      }
      ()
    }

  override def markAuditFailed(
    requestId: String,
    decisionId: String,
    reason: String,
    maxAttempts: Int,
    retryDelaySeconds: Long
  ): Future[OutboxDeliveryResult] =
    Future.successful {
      val nextStatus = auditRows.get((requestId, decisionId)) match {
        case Some(row) =>
          val attempts = row.attempts + 1
          val status = if (attempts >= maxAttempts) "dead" else "retry"
          val nextRetryAt = if (status == "dead") None else Some(System.currentTimeMillis() + retryDelaySeconds * 1000L)
          auditRows.put((requestId, decisionId), row.copy(status = status, attempts = attempts, lastError = Some(reason), nextRetryAt = nextRetryAt))
          status
        case None => "dead"
      }
      if (nextStatus == "dead") OutboxDeliveryResult.DeadLettered else OutboxDeliveryResult.ScheduledRetry
    }
}

private final case class InMemoryOutboxRow(
  event: TransitionEvent,
  status: String,
  attempts: Int,
  lastError: Option[String],
  nextRetryAt: Option[Long]
)
