package com.kofiska.solana.orchestrator.domain

final case class IngressRouteCandidate(
  routeId: String,
  venue: String,
  hopCount: Int
)

final case class IngressRequest(
  requestId: String,
  dedupeKey: String,
  traceId: String,
  modelVersion: String,
  tokenIn: String,
  tokenOut: String,
  amountIn: String,
  routeId: Option[String],
  slot: Long,
  quoteAge: Long,
  sourceHashes: Vector[String],
  routeCandidates: Vector[IngressRouteCandidate]
)

final case class IngressDecision(
  requestId: String,
  decisionId: String,
  terminalState: String,
  reasonCode: String,
  actionability: String,
  bestRouteId: Option[String],
  sourceHashes: Vector[String],
  expectedOutput: Option[String],
  feeCost: Option[String],
  slippageCost: Option[String],
  breakevenMargin: Option[String],
  evEstimate: Option[String],
  evLowerBound: Option[String],
  riskScore: Option[String],
  freshnessValid: Boolean
)

final case class IngressError(
  requestId: Option[String],
  reasonCode: String,
  message: String
)
