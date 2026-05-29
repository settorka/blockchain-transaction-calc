package com.kofiska.solana.orchestrator.service

import com.kofiska.solana.orchestrator.domain._
import com.kofiska.solana.orchestrator.ports.{AuditPublisher, ComputeGateway, DecisionRepository, DedupeCache}
import com.kofiska.solana.v1.decision.{Actionability => ProtoActionability, EvaluateSwapResponse, TerminalState => ProtoTerminalState}

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import scala.util.control.NonFatal

final class RequestWorkflow(
  computeGateway: ComputeGateway,
  decisionRepository: DecisionRepository,
  dedupeCache: DedupeCache,
  auditPublisher: AuditPublisher,
  dedupeTtlSeconds: Long
)(implicit ec: ExecutionContext) {

  def process(ctx: RequestContext): Future[DecisionResult] =
    dedupeCache.claim(ctx.dedupeKey, inflightMarker(ctx), dedupeTtlSeconds).flatMap {
      case true =>
        val computeResult = Try(computeGateway.evaluate(ctx)).getOrElse(
          Future.failed(new IllegalStateException("compute gateway failed to start"))
        )
        computeResult
          .map(response => Right(response): Either[DecisionResult, EvaluateSwapResponse])
          .recover {
            case NonFatal(error) => Left(failedComputeResult(ctx, error.getMessage))
          }
          .flatMap {
            case Right(response) =>
              val result = validatedResultFromResponse(ctx, response)
              persistAndAudit(ctx, result, response.computeLatencyMs.toLong)
            case Left(result) =>
              persistAndAudit(ctx, result, 0L)
          }

      case false =>
        findDurable(ctx).flatMap {
          case Some(result) =>
            auditPublisher
              .publish(replayEvent(ctx, result))
              .flatMap(_ => decisionRepository.markAuditPublished(result.requestId, result.decisionId))
              .recover { case _ => () }
              .map(_ => result)
          case None =>
            Future.successful(duplicateInFlightResult(ctx))
        }
    }

  private def persistAndAudit(
    ctx: RequestContext,
    result: DecisionResult,
    computeLatencyMs: Long
  ): Future[DecisionResult] = {
    val event = transitionEvent(ctx, result, computeLatencyMs)
    for {
      durable <- decisionRepository.upsert(ctx, result, event)
      auditEvent = if (durable.requestId == result.requestId && durable.decisionId == result.decisionId) {
        transitionEvent(ctx.copy(requestId = durable.requestId), durable, computeLatencyMs)
      } else {
        replayEvent(ctx, durable)
      }
      _ <- auditPublisher
        .publish(auditEvent)
        .flatMap(_ => decisionRepository.markAuditPublished(durable.requestId, durable.decisionId))
        .recover { case _ => () }
      _ <- dedupeCache.put(ctx.dedupeKey, durable.decisionId, ttlSeconds = dedupeTtlSeconds).recover { case _ => () }
    } yield durable
  }

  private def findDurable(ctx: RequestContext): Future[Option[DecisionResult]] =
    decisionRepository.findByDedupeKey(ctx.dedupeKey).flatMap {
      case some @ Some(_) => Future.successful(some)
      case None           => decisionRepository.find(ctx.requestId)
    }

  private def validatedResultFromResponse(ctx: RequestContext, response: EvaluateSwapResponse): DecisionResult =
    if (response.requestId != ctx.requestId) {
      failedComputeResult(ctx, "COMPUTE_RESPONSE_REQUEST_ID_MISMATCH")
    } else if (response.sourceHashes.toVector != ctx.sourceHashes) {
      failedComputeResult(ctx, "COMPUTE_RESPONSE_SOURCE_HASH_MISMATCH")
    } else {
      resultFromResponse(ctx, response)
    }

  private def resultFromResponse(ctx: RequestContext, response: EvaluateSwapResponse): DecisionResult = {
    val terminalState = response.terminalState match {
      case ProtoTerminalState.ACCEPT => TerminalState.Accept
      case ProtoTerminalState.DEFER   => TerminalState.Defer
      case ProtoTerminalState.REJECT  => TerminalState.Reject
      case ProtoTerminalState.FAILED  => TerminalState.Failed
      case _                         => TerminalState.Failed
    }

    val actionability = response.actionability match {
      case ProtoActionability.ACTIONABLE     => Actionability.Actionable
      case ProtoActionability.NON_ACTIONABLE => Actionability.NonActionable
      case ProtoActionability.STALE          => Actionability.Stale
      case ProtoActionability.CONFLICT       => Actionability.Conflict
      case _                                 => Actionability.NonActionable
    }

    DecisionResult(
      requestId = response.requestId,
      decisionId = if (response.decisionId.nonEmpty) response.decisionId else deterministicDecisionId(ctx.requestId, ctx.modelVersion, response.reasonCode),
      terminalState = terminalState,
      reasonCode = response.reasonCode,
      actionability = actionability,
      bestRouteId = Option(response.bestRouteId).filter(_.nonEmpty),
      sourceHashes = response.sourceHashes.toVector,
      expectedOutput = decimalOption(response.expectedOutput),
      feeCost = decimalOption(response.feeCost),
      slippageCost = decimalOption(response.slippageCost),
      breakevenMargin = decimalOption(response.breakevenMargin),
      evEstimate = decimalOption(response.evEstimate),
      evLowerBound = decimalOption(response.evLowerBound),
      riskScore = decimalOption(response.riskScore),
      freshnessValid = response.freshnessValid
    )
  }

  private def duplicateInFlightResult(ctx: RequestContext): DecisionResult =
    DecisionResult(
      requestId = ctx.requestId,
      decisionId = inflightMarker(ctx),
      terminalState = TerminalState.Failed,
      reasonCode = "DUPLICATE_INFLIGHT_REQUEST",
      actionability = Actionability.NonActionable,
      bestRouteId = ctx.routeId,
      sourceHashes = ctx.sourceHashes,
      expectedOutput = None,
      feeCost = None,
      slippageCost = None,
      breakevenMargin = None,
      evEstimate = None,
      evLowerBound = None,
      riskScore = None,
      freshnessValid = false
    )

  private def failedComputeResult(ctx: RequestContext, reason: String): DecisionResult =
    DecisionResult(
      requestId = ctx.requestId,
      decisionId = deterministicDecisionId(ctx.requestId, ctx.modelVersion, "FAILED_COMPUTE"),
      terminalState = TerminalState.Failed,
      reasonCode = if (reason.nonEmpty) reason else "COMPUTE_FAILED",
      actionability = Actionability.NonActionable,
      bestRouteId = ctx.routeId,
      sourceHashes = ctx.sourceHashes,
      expectedOutput = None,
      feeCost = None,
      slippageCost = None,
      breakevenMargin = None,
      evEstimate = None,
      evLowerBound = None,
      riskScore = None,
      freshnessValid = false
    )

  private def transitionEvent(
    ctx: RequestContext,
    result: DecisionResult,
    computeLatencyMs: Long
  ): TransitionEvent =
    TransitionEvent(
      schemaVersion = "v1",
      traceId = ctx.traceId,
      requestId = ctx.requestId,
      decisionId = result.decisionId,
      terminalState = TerminalState.asString(result.terminalState),
      reasonCode = result.reasonCode,
      modelVersion = ctx.modelVersion,
      routeId = result.bestRouteId,
      slot = ctx.slot,
      quoteAge = ctx.quoteAge,
      sourceHashes = ctx.sourceHashes,
      stage = "terminal",
      latencyMs = computeLatencyMs,
      bytesIn = requestBytes(ctx),
      bytesOut = resultBytes(result),
      success = result.terminalState != TerminalState.Failed
    )

  private def replayEvent(ctx: RequestContext, result: DecisionResult): TransitionEvent =
    TransitionEvent(
      schemaVersion = "v1",
      traceId = ctx.traceId,
      requestId = ctx.requestId,
      decisionId = result.decisionId,
      terminalState = TerminalState.asString(result.terminalState),
      reasonCode = "REPLAY_HIT",
      modelVersion = ctx.modelVersion,
      routeId = result.bestRouteId,
      slot = ctx.slot,
      quoteAge = ctx.quoteAge,
      sourceHashes = result.sourceHashes,
      stage = "replay",
      latencyMs = 0L,
      bytesIn = requestBytes(ctx),
      bytesOut = resultBytes(result),
      success = true
    )

  private def inflightMarker(ctx: RequestContext): String =
    s"inflight:${ctx.requestId}:${ctx.modelVersion}"

  private def deterministicDecisionId(parts: String*): String =
    UUID.nameUUIDFromBytes(parts.mkString(":").getBytes(StandardCharsets.UTF_8)).toString

  private def requestBytes(ctx: RequestContext): Long =
    Seq(
      ctx.requestId,
      ctx.dedupeKey,
      ctx.traceId,
      ctx.modelVersion,
      ctx.tokenIn,
      ctx.tokenOut,
      ctx.amountIn,
      ctx.routeId.getOrElse(""),
      ctx.sourceHashes.mkString(",")
    ).map(_.getBytes(StandardCharsets.UTF_8).length.toLong).sum

  private def resultBytes(result: DecisionResult): Long =
    Seq(
      result.requestId,
      result.decisionId,
      TerminalState.asString(result.terminalState),
      result.reasonCode,
      Actionability.asString(result.actionability),
      result.bestRouteId.getOrElse(""),
      result.sourceHashes.mkString(","),
      decimalString(result.expectedOutput),
      decimalString(result.feeCost),
      decimalString(result.slippageCost),
      decimalString(result.breakevenMargin),
      decimalString(result.evEstimate),
      decimalString(result.evLowerBound),
      decimalString(result.riskScore)
    ).map(_.getBytes(StandardCharsets.UTF_8).length.toLong).sum

  private def decimalOption(value: String): Option[BigDecimal] =
    Option(value).map(_.trim).filter(_.nonEmpty).flatMap(v => Try(BigDecimal(v)).toOption)

  private def decimalString(value: Option[BigDecimal]): String =
    value.map(_.bigDecimal.toPlainString).getOrElse("")
}
