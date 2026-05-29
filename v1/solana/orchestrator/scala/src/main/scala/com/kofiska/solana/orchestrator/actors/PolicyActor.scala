package com.kofiska.solana.orchestrator.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.kofiska.solana.orchestrator.domain.RequestContext

object PolicyActor {
  sealed trait Command
  final case class Evaluate(ctx: RequestContext, replyTo: ActorRef[Result]) extends Command

  sealed trait Result
  case object Allowed extends Result
  final case class Rejected(reason: String) extends Result

  def apply(): Behavior[Command] =
    Behaviors.receiveMessage {
      case Evaluate(ctx, replyTo) =>
        val result =
          if (ctx.requestId.trim.isEmpty || ctx.dedupeKey.trim.isEmpty) Rejected("MISSING_IDENTITY")
          else if (ctx.routeCandidates.size > 16) Rejected("TOO_MANY_ROUTE_CANDIDATES")
          else Allowed
        replyTo ! result
        Behaviors.same
    }
}
