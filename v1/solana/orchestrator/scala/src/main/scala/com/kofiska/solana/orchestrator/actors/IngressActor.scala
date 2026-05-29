package com.kofiska.solana.orchestrator.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.kofiska.solana.orchestrator.domain.{Actionability, DecisionResult, RequestContext, TerminalState}
import com.kofiska.solana.orchestrator.service.RequestWorkflow

import java.nio.charset.StandardCharsets
import java.util.UUID
import scala.util.{Failure, Success}

object IngressActor {
  sealed trait Command
  final case class Submit(ctx: RequestContext, replyTo: ActorRef[DecisionResult]) extends Command
  private final case class Completed(result: DecisionResult, replyTo: ActorRef[DecisionResult]) extends Command

  def apply(workflow: RequestWorkflow, maxInFlight: Int = 1024): Behavior[Command] =
    Behaviors.setup { context =>
      def active(inFlight: Int): Behavior[Command] = Behaviors.receiveMessage {
        case Submit(ctx, replyTo) =>
          if (inFlight >= maxInFlight) {
            replyTo ! overloadResult(ctx)
            Behaviors.same
          } else {
            context.log.info("ingress request {}", ctx.requestId)
            context.pipeToSelf(workflow.process(ctx)) {
              case Success(result) => Completed(result, replyTo)
              case Failure(error)  => Completed(failureResult(ctx, error.getMessage), replyTo)
            }
            active(inFlight + 1)
          }

        case Completed(result, replyTo) =>
          replyTo ! result
          active(math.max(0, inFlight - 1))
      }

      active(0)
    }

  private def overloadResult(ctx: RequestContext): DecisionResult =
    failureResult(ctx, "INGRESS_OVERLOADED")

  private def failureResult(ctx: RequestContext, reason: String): DecisionResult =
    DecisionResult(
      requestId = ctx.requestId,
      decisionId = UUID.nameUUIDFromBytes(s"${ctx.requestId}:${ctx.modelVersion}:$reason".getBytes(StandardCharsets.UTF_8)).toString,
      terminalState = TerminalState.Failed,
      reasonCode = reason,
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
}
