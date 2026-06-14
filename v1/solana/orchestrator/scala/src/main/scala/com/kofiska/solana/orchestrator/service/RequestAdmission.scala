package com.kofiska.solana.orchestrator.service

import com.kofiska.solana.orchestrator.domain._

final case class IngressBounds(
  maxRequestBytes: Long,
  maxRouteCandidates: Int,
  maxSourceHashes: Int
)

object RequestAdmission {
  def validate(request: IngressRequest, bounds: IngressBounds): Either[IngressError, RequestContext] =
    for {
      _ <- validateRequired(request)
      _ <- validateLimits(request, bounds)
    } yield toContext(request)

  private def validateRequired(request: IngressRequest): Either[IngressError, Unit] =
    requiredFields(request).collectFirst {
      case (name, value) if value.trim.isEmpty =>
        Left(IngressError(Some(request.requestId), "INVALID_REQUEST", s"$name must not be empty"))
    }.getOrElse(Right(()))

  private def validateLimits(request: IngressRequest, bounds: IngressBounds): Either[IngressError, Unit] =
    if (serializedSize(request) > bounds.maxRequestBytes) {
      Left(IngressError(Some(request.requestId), "REQUEST_TOO_LARGE", "request body exceeds max request bytes"))
    } else if (request.routeCandidates.size > bounds.maxRouteCandidates) {
      Left(IngressError(Some(request.requestId), "TOO_MANY_ROUTE_CANDIDATES", "route candidate limit exceeded"))
    } else if (request.sourceHashes.size > bounds.maxSourceHashes) {
      Left(IngressError(Some(request.requestId), "TOO_MANY_SOURCE_HASHES", "source hash limit exceeded"))
    } else if (request.routeCandidates.exists(_.hopCount <= 0)) {
      Left(IngressError(Some(request.requestId), "INVALID_ROUTE_CANDIDATE", "hop count must be positive"))
    } else if (request.slot < 0 || request.quoteAge < 0) {
      Left(IngressError(Some(request.requestId), "INVALID_FRESHNESS", "slot and quote age must be non-negative"))
    } else {
      Right(())
    }

  private def requiredFields(request: IngressRequest): Seq[(String, String)] =
    Seq(
      "requestId" -> request.requestId,
      "dedupeKey" -> request.dedupeKey,
      "traceId" -> request.traceId,
      "modelVersion" -> request.modelVersion,
      "tokenIn" -> request.tokenIn,
      "tokenOut" -> request.tokenOut,
      "amountIn" -> request.amountIn
    )

  private def serializedSize(request: IngressRequest): Long =
    Seq(
      request.requestId,
      request.dedupeKey,
      request.traceId,
      request.modelVersion,
      request.tokenIn,
      request.tokenOut,
      request.amountIn,
      request.routeId.getOrElse(""),
      request.slot.toString,
      request.quoteAge.toString,
      request.sourceHashes.mkString(","),
      request.routeCandidates.map(candidate => s"${candidate.routeId}:${candidate.venue}:${candidate.hopCount}").mkString(",")
    ).map(_.getBytes(java.nio.charset.StandardCharsets.UTF_8).length.toLong).sum

  private def toContext(request: IngressRequest): RequestContext =
    RequestContext(
      requestId = request.requestId,
      dedupeKey = request.dedupeKey,
      traceId = request.traceId,
      modelVersion = request.modelVersion,
      tokenIn = request.tokenIn,
      tokenOut = request.tokenOut,
      amountIn = request.amountIn,
      routeId = request.routeId,
      slot = request.slot,
      quoteAge = request.quoteAge,
      sourceHashes = request.sourceHashes,
      routeCandidates = request.routeCandidates.map(candidate =>
        RouteCandidateInput(candidate.routeId, candidate.venue, candidate.hopCount)
      )
    )
}
