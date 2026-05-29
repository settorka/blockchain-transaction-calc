package com.kofiska.solana.orchestrator.actors

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.kofiska.solana.orchestrator.ports.DedupeCache

import scala.util.{Failure, Success}

object DedupeActor {
  sealed trait Command
  final case class Check(dedupeKey: String, marker: String, ttlSeconds: Long, replyTo: ActorRef[Result]) extends Command
  private final case class Checked(result: Result, replyTo: ActorRef[Result]) extends Command

  sealed trait Result
  case object Claimed extends Result
  final case class Existing(value: Option[String]) extends Result
  final case class Failed(reason: String) extends Result

  def apply(cache: DedupeCache): Behavior[Command] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage {
        case Check(dedupeKey, marker, ttlSeconds, replyTo) =>
          context.pipeToSelf(cache.claim(dedupeKey, marker, ttlSeconds)) {
            case Success(true) => Checked(Claimed, replyTo)
            case Success(false) =>
              Checked(Existing(None), replyTo)
            case Failure(error) => Checked(Failed(error.getMessage), replyTo)
          }
          Behaviors.same

        case Checked(result, replyTo) =>
          replyTo ! result
          Behaviors.same
      }
    }
}
