package com.kofiska.solana.orchestrator.domain

final case class PoolSnapshot(
  active: Int,
  idle: Int,
  total: Int,
  waiting: Int
)

final case class AuditBacklogSnapshot(
  pendingCount: Long,
  oldestAgeMs: Long
)

final case class RuntimeMetricsSnapshot(
  ingressRequests: Long,
  ingressAccepted: Long,
  ingressRejected: Long,
  ingressOverloaded: Long,
  inflightCurrent: Int,
  requestLatencyTotalMs: Long,
  requestLatencySamples: Long,
  requestLatencyMaxMs: Long,
  computeLatencyTotalMs: Long,
  computeLatencySamples: Long,
  computeLatencyMaxMs: Long,
  terminalAcceptCount: Long,
  terminalDeferCount: Long,
  terminalRejectCount: Long,
  terminalFailedCount: Long,
  replayHits: Long,
  replayDrift: Long,
  auditPublishFailures: Long,
  auditRetryScheduled: Long,
  auditDeadLetters: Long,
  dedupeRepairs: Long,
  auditBacklog: AuditBacklogSnapshot,
  pool: PoolSnapshot
)
