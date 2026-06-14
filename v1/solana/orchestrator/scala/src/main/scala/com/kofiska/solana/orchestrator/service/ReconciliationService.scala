package com.kofiska.solana.orchestrator.service

import com.kofiska.solana.orchestrator.domain.OutboxDeliveryResult
import com.kofiska.solana.orchestrator.ports.{AuditPublisher, DecisionRepository, DedupeCache}

import scala.concurrent.{ExecutionContext, Future}

final class ReconciliationService(
  decisionRepository: DecisionRepository,
  dedupeCache: DedupeCache,
  auditPublisher: AuditPublisher,
  dedupeTtlSeconds: Long,
  auditMaxAttempts: Int,
  auditRetryDelaySeconds: Long,
  metrics: RuntimeMetrics = RuntimeMetrics.noop
)(implicit ec: ExecutionContext) {

  def runOnce(limit: Int): Future[ReconciliationReport] =
    for {
      pending <- decisionRepository.pendingAudit(limit)
      audit <- publishPendingAudit(pending)
      dedupeDrift <- repairDedupe(limit)
    } yield ReconciliationReport(
      pendingAuditCount = pending.size,
      republishedAuditCount = audit._1,
      auditPublishFailureCount = audit._2,
      auditDeadLetterCount = audit._3,
      auditRetryScheduledCount = audit._4,
      dedupeRepairedCount = dedupeDrift
    )

  private def publishPendingAudit(
    events: Vector[com.kofiska.solana.orchestrator.domain.TransitionEvent]
  ): Future[(Int, Int, Int, Int)] =
    events.foldLeft(Future.successful((0, 0, 0, 0))) { (acc, event) =>
      acc.flatMap { case (published, failed, dead, retry) =>
        auditPublisher.publish(event).flatMap { _ =>
          decisionRepository.markAuditPublished(event.requestId, event.decisionId).map(_ => (published + 1, failed, dead, retry))
        }.recoverWith { case error =>
          metrics.recordAuditPublishFailure()
          decisionRepository
            .markAuditFailed(event.requestId, event.decisionId, auditError(error), auditMaxAttempts, auditRetryDelaySeconds)
            .map {
              case OutboxDeliveryResult.DeadLettered =>
                metrics.recordAuditDeadLetter()
                (published, failed + 1, dead + 1, retry)
              case OutboxDeliveryResult.ScheduledRetry =>
                metrics.recordAuditRetryScheduled()
                (published, failed + 1, dead, retry + 1)
            }
            .recover { case _ => (published, failed + 1, dead, retry) }
        }
      }
    }

  private def repairDedupe(limit: Int): Future[Int] =
    for {
      keys <- dedupeCache.scan("solana:dedupe:", limit)
      repaired <- keys.foldLeft(Future.successful(0)) { (acc, key) =>
        acc.flatMap { count =>
          val dedupeKey = key.stripPrefix("solana:dedupe:")
          decisionRepository.findByDedupeKey(dedupeKey).flatMap {
            case Some(result) =>
              metrics.recordDedupeRepair()
              dedupeCache.put(dedupeKey, result.decisionId, dedupeTtlSeconds).map(_ => count + 1)
            case None =>
              dedupeCache.delete(dedupeKey).map(_ => count)
          }
        }
      }
    } yield repaired

  private def auditError(error: Throwable): String =
    Option(error.getMessage).filter(_.nonEmpty).getOrElse(error.getClass.getSimpleName)
}

final case class ReconciliationReport(
  pendingAuditCount: Int,
  republishedAuditCount: Int,
  auditPublishFailureCount: Int,
  auditDeadLetterCount: Int,
  auditRetryScheduledCount: Int,
  dedupeRepairedCount: Int
)
