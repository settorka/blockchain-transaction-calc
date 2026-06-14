package com.kofiska.solana.orchestrator.infra.http

import com.kofiska.solana.orchestrator.domain._
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json._

trait JsonSupport extends DefaultJsonProtocol {
  implicit val ingressRouteCandidateFormat: RootJsonFormat[IngressRouteCandidate] = jsonFormat3(IngressRouteCandidate)
  implicit val ingressRequestFormat: RootJsonFormat[IngressRequest] = jsonFormat12(IngressRequest)
  implicit val ingressDecisionFormat: RootJsonFormat[IngressDecision] = jsonFormat15(IngressDecision)
  implicit val ingressErrorFormat: RootJsonFormat[IngressError] = jsonFormat3(IngressError)
  implicit val poolSnapshotFormat: RootJsonFormat[PoolSnapshot] = jsonFormat4(PoolSnapshot)
  implicit val auditBacklogSnapshotFormat: RootJsonFormat[AuditBacklogSnapshot] = jsonFormat2(AuditBacklogSnapshot)
  implicit val runtimeMetricsSnapshotFormat: RootJsonFormat[RuntimeMetricsSnapshot] = new RootJsonFormat[RuntimeMetricsSnapshot] {
    override def write(value: RuntimeMetricsSnapshot): JsValue =
      JsObject(
        "ingress_requests" -> JsNumber(value.ingressRequests),
        "ingress_accepted" -> JsNumber(value.ingressAccepted),
        "ingress_rejected" -> JsNumber(value.ingressRejected),
        "ingress_overloaded" -> JsNumber(value.ingressOverloaded),
        "inflight_current" -> JsNumber(value.inflightCurrent),
        "request_latency_total_ms" -> JsNumber(value.requestLatencyTotalMs),
        "request_latency_samples" -> JsNumber(value.requestLatencySamples),
        "request_latency_max_ms" -> JsNumber(value.requestLatencyMaxMs),
        "compute_latency_total_ms" -> JsNumber(value.computeLatencyTotalMs),
        "compute_latency_samples" -> JsNumber(value.computeLatencySamples),
        "compute_latency_max_ms" -> JsNumber(value.computeLatencyMaxMs),
        "terminal_accept_count" -> JsNumber(value.terminalAcceptCount),
        "terminal_defer_count" -> JsNumber(value.terminalDeferCount),
        "terminal_reject_count" -> JsNumber(value.terminalRejectCount),
        "terminal_failed_count" -> JsNumber(value.terminalFailedCount),
        "replay_hits" -> JsNumber(value.replayHits),
        "replay_drift" -> JsNumber(value.replayDrift),
        "audit_publish_failures" -> JsNumber(value.auditPublishFailures),
        "audit_retry_scheduled" -> JsNumber(value.auditRetryScheduled),
        "audit_dead_letters" -> JsNumber(value.auditDeadLetters),
        "dedupe_repairs" -> JsNumber(value.dedupeRepairs),
        "audit_backlog" -> auditBacklogSnapshotFormat.write(value.auditBacklog),
        "pool" -> poolSnapshotFormat.write(value.pool)
      )

    override def read(value: JsValue): RuntimeMetricsSnapshot = {
      val obj = value.asJsObject
      RuntimeMetricsSnapshot(
        ingressRequests = fieldLong(obj, "ingress_requests"),
        ingressAccepted = fieldLong(obj, "ingress_accepted"),
        ingressRejected = fieldLong(obj, "ingress_rejected"),
        ingressOverloaded = fieldLong(obj, "ingress_overloaded"),
        inflightCurrent = fieldInt(obj, "inflight_current"),
        requestLatencyTotalMs = fieldLong(obj, "request_latency_total_ms"),
        requestLatencySamples = fieldLong(obj, "request_latency_samples"),
        requestLatencyMaxMs = fieldLong(obj, "request_latency_max_ms"),
        computeLatencyTotalMs = fieldLong(obj, "compute_latency_total_ms"),
        computeLatencySamples = fieldLong(obj, "compute_latency_samples"),
        computeLatencyMaxMs = fieldLong(obj, "compute_latency_max_ms"),
        terminalAcceptCount = fieldLong(obj, "terminal_accept_count"),
        terminalDeferCount = fieldLong(obj, "terminal_defer_count"),
        terminalRejectCount = fieldLong(obj, "terminal_reject_count"),
        terminalFailedCount = fieldLong(obj, "terminal_failed_count"),
        replayHits = fieldLong(obj, "replay_hits"),
        replayDrift = fieldLong(obj, "replay_drift"),
        auditPublishFailures = fieldLong(obj, "audit_publish_failures"),
        auditRetryScheduled = fieldLong(obj, "audit_retry_scheduled"),
        auditDeadLetters = fieldLong(obj, "audit_dead_letters"),
        dedupeRepairs = fieldLong(obj, "dedupe_repairs"),
        auditBacklog = obj.fields("audit_backlog").convertTo[AuditBacklogSnapshot],
        pool = obj.fields("pool").convertTo[PoolSnapshot]
      )
    }
  }

  private def fieldLong(obj: JsObject, name: String): Long =
    obj.fields(name) match {
      case JsNumber(value) => value.toLong
      case JsString(value)  => value.toLong
      case other            => deserializationError(s"field $name must be numeric, got $other")
    }

  private def fieldInt(obj: JsObject, name: String): Int =
    fieldLong(obj, name).toInt
}
