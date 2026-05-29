package com.kofiska.solana.orchestrator.service

import com.kofiska.solana.orchestrator.ports.{AuditPublisher, DecisionRepository, DedupeCache}

import scala.concurrent.{ExecutionContext, Future}

final class ReconciliationService(
  decisionRepository: DecisionRepository,
  dedupeCache: DedupeCache,
  auditPublisher: AuditPublisher,
  dedupeTtlSeconds: Long
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
      dedupeRepairedCount = dedupeDrift
    )

  private def publishPendingAudit(events: Vector[com.kofiska.solana.orchestrator.domain.TransitionEvent]): Future[(Int, Int)] =
    events.foldLeft(Future.successful((0, 0))) { (acc, event) =>
      acc.flatMap { case (published, failed) =>
        auditPublisher.publish(event).flatMap { _ =>
          decisionRepository.markAuditPublished(event.requestId, event.decisionId).map(_ => (published + 1, failed))
        }.recover { case _ => (published, failed + 1) }
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
              dedupeCache.put(dedupeKey, result.decisionId, dedupeTtlSeconds).map(_ => count + 1)
            case None =>
              dedupeCache.delete(dedupeKey).map(_ => count)
          }
        }
      }
    } yield repaired
}

final case class ReconciliationReport(
  pendingAuditCount: Int,
  republishedAuditCount: Int,
  auditPublishFailureCount: Int,
  dedupeRepairedCount: Int
)
