package com.kofiska.solana.orchestrator

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import akka.actor.typed.scaladsl.adapter._
import com.kofiska.solana.orchestrator.config.AppConfig
import com.kofiska.solana.orchestrator.domain._
import com.kofiska.solana.orchestrator.infra.http.{IngressHttpServer, JsonSupport}
import com.kofiska.solana.orchestrator.ports.{AuditPublisher, ComputeGateway, DecisionRepository, DedupeCache}
import com.kofiska.solana.orchestrator.service.{RequestWorkflow, RuntimeMetrics}
import com.kofiska.solana.v1.decision.{Actionability => ProtoActionability, EvaluateSwapResponse, TerminalState => ProtoTerminalState}
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import spray.json._

import java.net.URI
import java.net.http.{HttpClient, HttpRequest, HttpResponse}
import java.util.concurrent.ConcurrentHashMap
import scala.collection.concurrent.TrieMap
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._

final class OrchestratorIntegrationSpec extends AsyncFlatSpec with Matchers with JsonSupport {
  implicit override def executionContext: ExecutionContext = ExecutionContext.global

  it should "serve decisions, replay after restart, and expose runtime metrics" in Future {
    val system = ActorSystem(Behaviors.empty[Unit], "orchestrator-integration")
    val metrics = RuntimeMetrics.live()
    val repository = new SharedDecisionRepository
    val cache = new SharedDedupeCache
    val audit = new RecordingAuditPublisher
    val workflow = new RequestWorkflow(
      new CountingGateway,
      repository,
      cache,
      audit,
      dedupeTtlSeconds = 3600,
      auditMaxAttempts = 3,
      auditRetryDelaySeconds = 30,
      metrics = metrics
    )(executionContext)

    val config = baseConfig.copy(httpPort = 0)
    val binding1 = Await.result(
      IngressHttpServer.start(system.toClassic, workflow, repository, config, metrics)(executionContext),
      10.seconds
    )
    val port1 = binding1.localAddress.getPort

    val request = ingressRequest("req-1", "dedupe-1")
    val first = postJson(port1, request)
    first.statusCode() shouldBe 200
    val firstDecision = first.body().parseJson.convertTo[IngressDecision]
    firstDecision.terminalState shouldBe "ACCEPT"
    firstDecision.decisionId shouldBe "decision-1"

    Await.result(binding1.unbind(), 10.seconds)

    val workflow2 = new RequestWorkflow(
      new FailingGateway,
      repository,
      cache,
      audit,
      dedupeTtlSeconds = 3600,
      auditMaxAttempts = 3,
      auditRetryDelaySeconds = 30,
      metrics = metrics
    )(executionContext)
    val binding2 = Await.result(
      IngressHttpServer.start(system.toClassic, workflow2, repository, config, metrics)(executionContext),
      10.seconds
    )
    val port2 = binding2.localAddress.getPort

    val second = postJson(port2, request)
    second.statusCode() shouldBe 200
    val secondDecision = second.body().parseJson.convertTo[IngressDecision]
    secondDecision.decisionId shouldBe "decision-1"
    secondDecision.reasonCode shouldBe "ACCEPTED"

    val metricsResponse = getJson(port2, "/v1/metrics")
    metricsResponse.statusCode() shouldBe 200
    val snapshot = metricsResponse.body().parseJson.convertTo[RuntimeMetricsSnapshot]
    snapshot.ingressRequests shouldBe 2L
    snapshot.replayHits shouldBe 1L
    snapshot.replayDrift shouldBe 0L
    snapshot.auditBacklog.pendingCount shouldBe 0L
    snapshot.pool.total shouldBe 0
    snapshot.computeLatencySamples shouldBe 1L
    audit.events.exists(_.stage == "replay") shouldBe true

    Await.result(binding2.unbind(), 10.seconds)
    system.terminate()

    succeed
  }

  private val baseConfig = AppConfig(
    httpHost = "127.0.0.1",
    httpPort = 8080,
    ingressMaxInFlight = 8,
    ingressRequestTimeoutMs = 5000L,
    maxRequestBytes = 65536L,
    maxRouteCandidates = 16,
    maxSourceHashes = 16,
    computeHost = "127.0.0.1",
    computePort = 50051,
    postgresUrl = "jdbc:postgresql://127.0.0.1:5432/decision_store",
    postgresUser = "decision",
    postgresPassword = "decision",
    postgresPoolSize = 2,
    postgresConnectionTimeoutMs = 5000L,
    valkeyUri = "redis://127.0.0.1:6379/0",
    dedupeTtlSeconds = 3600L,
    auditMaxAttempts = 3,
    auditRetryDelaySeconds = 30L,
    auditBootstrapServers = "127.0.0.1:9092",
    auditTopic = "solana.audit"
  )

  private def ingressRequest(requestId: String, dedupeKey: String): String =
    s"""{
       |"requestId":"$requestId",
       |"dedupeKey":"$dedupeKey",
       |"traceId":"trace-1",
       |"modelVersion":"v1",
       |"tokenIn":"USDC",
       |"tokenOut":"SOL",
       |"amountIn":"1000",
       |"routeId":"route-a",
       |"slot":100,
       |"quoteAge":1,
       |"sourceHashes":["hash-a","hash-b"],
       |"routeCandidates":[
       |  {"routeId":"route-a","venue":"direct","hopCount":1},
       |  {"routeId":"route-b","venue":"aggregator","hopCount":2}
       |]
       |}""".stripMargin

  private def postJson(port: Int, body: String): HttpResponse[String] = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$port/v1/decisions:preflight"))
      .header("Content-Type", "application/json")
      .POST(HttpRequest.BodyPublishers.ofString(body))
      .build()
    client.send(request, HttpResponse.BodyHandlers.ofString())
  }

  private def getJson(port: Int, path: String): HttpResponse[String] = {
    val client = HttpClient.newHttpClient()
    val request = HttpRequest.newBuilder()
      .uri(URI.create(s"http://127.0.0.1:$port$path"))
      .GET()
      .build()
    client.send(request, HttpResponse.BodyHandlers.ofString())
  }

  private def acceptResponse(requestId: String): EvaluateSwapResponse =
    EvaluateSwapResponse(
      requestId = requestId,
      decisionId = "decision-1",
      terminalState = ProtoTerminalState.ACCEPT,
      actionability = ProtoActionability.ACTIONABLE,
      reasonCode = "ACCEPTED",
      bestRouteId = "route-a",
      expectedOutput = "1016.200000",
      feeCost = "0.780000",
      slippageCost = "1.070000",
      breakevenMargin = "14.350000",
      evEstimate = "13.700000",
      evLowerBound = "13.200000",
      riskScore = "0.001700",
      freshnessValid = true,
      computeLatencyMs = 7L,
      sourceHashes = Vector("hash-a", "hash-b")
    )

  private final class CountingGateway extends ComputeGateway {
    @volatile var count = 0

    override def evaluate(request: RequestContext): Future[EvaluateSwapResponse] = {
      count += 1
      Future.successful(acceptResponse(request.requestId))
    }
  }

  private final class FailingGateway extends ComputeGateway {
    override def evaluate(request: RequestContext): Future[EvaluateSwapResponse] =
      Future.failed(new IllegalStateException("compute should not run on replay"))
  }

  private final class SharedDecisionRepository extends DecisionRepository {
    val state = new ConcurrentHashMap[String, DecisionResult]()
    val dedupe = new ConcurrentHashMap[String, String]()
    val audit = TrieMap.empty[(String, String), TransitionEvent]
    val status = TrieMap.empty[(String, String), String]

    override def find(requestId: String): Future[Option[DecisionResult]] =
      Future.successful(Option(state.get(requestId)))

    override def findByDedupeKey(dedupeKey: String): Future[Option[DecisionResult]] =
      Future.successful(Option(dedupe.get(dedupeKey)).flatMap(id => Option(state.get(id))))

    override def upsert(ctx: RequestContext, result: DecisionResult, event: TransitionEvent): Future[DecisionResult] =
      Future.successful {
        val durableRequestId = Option(dedupe.putIfAbsent(ctx.dedupeKey, ctx.requestId)).getOrElse(ctx.requestId)
        val durable = Option(state.putIfAbsent(durableRequestId, result.copy(requestId = durableRequestId)))
          .getOrElse(state.get(durableRequestId))
        audit.putIfAbsent((durable.requestId, durable.decisionId), event.copy(requestId = durable.requestId, decisionId = durable.decisionId))
        status.put((durable.requestId, durable.decisionId), "pending")
        durable
      }

    override def pendingAudit(limit: Int): Future[Vector[TransitionEvent]] =
      Future.successful(
        audit.collect {
          case (key, event) if status.getOrElse(key, "pending") != "sent" => event
        }.take(limit).toVector
      )

    override def auditBacklogSnapshot(limit: Int): Future[AuditBacklogSnapshot] =
      Future.successful(AuditBacklogSnapshot(
        pendingCount = audit.count { case (key, _) => status.getOrElse(key, "pending") != "sent" }.toLong,
        oldestAgeMs = 0L
      ))

    override def connectionPoolSnapshot(): PoolSnapshot =
      PoolSnapshot(0, 0, 0, 0)

    override def markAuditPublished(requestId: String, decisionId: String): Future[Unit] =
      Future.successful {
        status.put((requestId, decisionId), "sent")
        ()
      }

    override def markAuditFailed(
      requestId: String,
      decisionId: String,
      reason: String,
      maxAttempts: Int,
      retryDelaySeconds: Long
    ): Future[OutboxDeliveryResult] =
      Future.successful(OutboxDeliveryResult.ScheduledRetry)
  }

  private final class SharedDedupeCache extends DedupeCache {
    val state = new ConcurrentHashMap[String, String]()

    override def get(requestId: String): Future[Option[String]] =
      Future.successful(Option(state.get(requestId)))

    override def claim(requestId: String, marker: String, ttlSeconds: Long): Future[Boolean] =
      Future.successful(state.putIfAbsent(requestId, marker) == null)

    override def put(requestId: String, decisionId: String, ttlSeconds: Long): Future[Unit] =
      Future.successful {
        state.put(requestId, decisionId)
        ()
      }

    override def delete(requestId: String): Future[Unit] =
      Future.successful {
        state.remove(requestId)
        ()
      }

    override def scan(prefix: String, limit: Int): Future[Vector[String]] =
      Future.successful(Vector.empty)
  }

  private final class RecordingAuditPublisher extends AuditPublisher {
    val events = scala.collection.mutable.ArrayBuffer.empty[TransitionEvent]

    override def publish(event: TransitionEvent): Future[Unit] =
      Future.successful {
        events += event
        ()
      }
  }
}
