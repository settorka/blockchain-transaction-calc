package com.kofiska.solana.orchestrator.domain

sealed trait OutboxDeliveryResult
object OutboxDeliveryResult {
  case object ScheduledRetry extends OutboxDeliveryResult
  case object DeadLettered extends OutboxDeliveryResult
}
