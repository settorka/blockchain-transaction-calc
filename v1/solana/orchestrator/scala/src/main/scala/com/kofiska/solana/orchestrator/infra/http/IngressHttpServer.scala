package com.kofiska.solana.orchestrator.infra.http

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import com.kofiska.solana.orchestrator.config.AppConfig
import com.kofiska.solana.orchestrator.domain._
import com.kofiska.solana.orchestrator.service.{IngressBounds, RequestAdmission, RequestWorkflow, RuntimeMetrics}

import java.util.concurrent.Semaphore
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

final class IngressHttpServer(
  system: ActorSystem,
  workflow: RequestWorkflow,
  decisionRepository: com.kofiska.solana.orchestrator.ports.DecisionRepository,
  config: AppConfig
  ,
  metrics: RuntimeMetrics
)(implicit ec: ExecutionContext)
    extends JsonSupport {

  private val admission = new Semaphore(config.ingressMaxInFlight)
  private implicit val classicSystemProvider: akka.actor.ClassicActorSystemProvider = system

  private val bounds = IngressBounds(
    maxRequestBytes = config.maxRequestBytes,
    maxRouteCandidates = config.maxRouteCandidates,
    maxSourceHashes = config.maxSourceHashes
  )

  def start(): Future[akka.http.scaladsl.Http.ServerBinding] =
    Http().newServerAt(config.httpHost, config.httpPort).bindFlow(Route.toFlow(route))

  private def route: Route =
    pathPrefix("v1") {
      concat(
        path("healthz") {
          get {
            complete(StatusCodes.OK -> """{"status":"ok"}""")
          }
        },
        path("readyz") {
          get {
            complete(StatusCodes.OK -> """{"status":"ready"}""")
          }
        },
        path("metrics") {
          get {
            onSuccess(decisionRepository.auditBacklogSnapshot(config.ingressMaxInFlight)) { auditBacklog =>
              val snapshot = metrics.snapshot(auditBacklog, decisionRepository.connectionPoolSnapshot())
              complete(StatusCodes.OK -> snapshot)
            }
          }
        },
        path("decisions:preflight") {
          post {
            val startedAt = System.nanoTime()
            metrics.recordIngressRequest()
            withRequestTimeout(config.ingressRequestTimeoutMs.millis) {
              withSizeLimit(config.maxRequestBytes) {
                entity(as[com.kofiska.solana.orchestrator.domain.IngressRequest]) { request =>
                  RequestAdmission.validate(request, bounds) match {
                    case Left(error) =>
                      metrics.recordIngressRejected()
                      metrics.recordRequestLatencyMs(nanosToMillis(System.nanoTime() - startedAt))
                      complete(errorStatus(error.reasonCode) -> error)
                    case Right(ctx) =>
                      if (!admission.tryAcquire()) {
                        metrics.recordIngressOverloaded()
                        metrics.recordRequestLatencyMs(nanosToMillis(System.nanoTime() - startedAt))
                        complete(StatusCodes.TooManyRequests -> com.kofiska.solana.orchestrator.domain.IngressError(
                          requestId = Some(ctx.requestId),
                          reasonCode = "INGRESS_OVERLOADED",
                          message = "too many in-flight requests"
                        ))
                      } else {
                        metrics.inFlightAcquired()
                        onComplete(workflow.process(ctx)) {
                          case Success(result) =>
                            admission.release()
                            metrics.inFlightReleased()
                            metrics.recordIngressAccepted()
                            metrics.recordRequestLatencyMs(nanosToMillis(System.nanoTime() - startedAt))
                            complete(StatusCodes.OK -> toIngressDecision(result))
                          case Failure(error) =>
                            admission.release()
                            metrics.inFlightReleased()
                            metrics.recordRequestLatencyMs(nanosToMillis(System.nanoTime() - startedAt))
                            complete(StatusCodes.InternalServerError -> com.kofiska.solana.orchestrator.domain.IngressError(
                              requestId = Some(ctx.requestId),
                              reasonCode = "INGRESS_FAILED",
                              message = Option(error.getMessage).getOrElse("ingress failed")
                            ))
                        }
                      }
                    }
                  }
                }
              }
          }
        }
      )
    }

  private def errorStatus(reasonCode: String) =
    reasonCode match {
      case "REQUEST_TOO_LARGE"       => StatusCodes.PayloadTooLarge
      case "TOO_MANY_ROUTE_CANDIDATES" => StatusCodes.BadRequest
      case "TOO_MANY_SOURCE_HASHES"  => StatusCodes.BadRequest
      case "INVALID_ROUTE_CANDIDATE" => StatusCodes.BadRequest
      case "INVALID_FRESHNESS"       => StatusCodes.BadRequest
      case "INVALID_REQUEST"         => StatusCodes.BadRequest
      case _                         => StatusCodes.BadRequest
    }

  private def toIngressDecision(result: DecisionResult): com.kofiska.solana.orchestrator.domain.IngressDecision =
    com.kofiska.solana.orchestrator.domain.IngressDecision(
      requestId = result.requestId,
      decisionId = result.decisionId,
      terminalState = TerminalState.asString(result.terminalState),
      reasonCode = result.reasonCode,
      actionability = com.kofiska.solana.orchestrator.domain.Actionability.asString(result.actionability),
      bestRouteId = result.bestRouteId,
      sourceHashes = result.sourceHashes,
      expectedOutput = result.expectedOutput.map(_.bigDecimal.toPlainString),
      feeCost = result.feeCost.map(_.bigDecimal.toPlainString),
      slippageCost = result.slippageCost.map(_.bigDecimal.toPlainString),
      breakevenMargin = result.breakevenMargin.map(_.bigDecimal.toPlainString),
      evEstimate = result.evEstimate.map(_.bigDecimal.toPlainString),
      evLowerBound = result.evLowerBound.map(_.bigDecimal.toPlainString),
      riskScore = result.riskScore.map(_.bigDecimal.toPlainString),
      freshnessValid = result.freshnessValid
    )

  private def nanosToMillis(value: Long): Long =
    value / 1000000L
}

object IngressHttpServer {
  def start(
    system: ActorSystem,
    workflow: RequestWorkflow,
    decisionRepository: com.kofiska.solana.orchestrator.ports.DecisionRepository,
    config: AppConfig,
    metrics: RuntimeMetrics
  )(implicit ec: ExecutionContext): Future[akka.http.scaladsl.Http.ServerBinding] =
    new IngressHttpServer(system, workflow, decisionRepository, config, metrics).start()
}
