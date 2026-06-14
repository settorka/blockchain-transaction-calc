package com.kofiska.solana.orchestrator.ports

import com.kofiska.solana.orchestrator.domain.{DecisionResult, RequestContext}
import com.kofiska.solana.orchestrator.domain.{OutboxDeliveryResult, TransitionEvent}

import scala.concurrent.Future

trait DecisionRepository {
  def find(requestId: String): Future[Option[DecisionResult]]
  def findByDedupeKey(dedupeKey: String): Future[Option[DecisionResult]]
  def upsert(ctx: RequestContext, result: DecisionResult, event: TransitionEvent): Future[DecisionResult]
  def pendingAudit(limit: Int): Future[Vector[TransitionEvent]]
  def markAuditPublished(requestId: String, decisionId: String): Future[Unit]
  def markAuditFailed(
    requestId: String,
    decisionId: String,
    reason: String,
    maxAttempts: Int,
    retryDelaySeconds: Long
  ): Future[OutboxDeliveryResult]
  def close(): Unit = ()
}
