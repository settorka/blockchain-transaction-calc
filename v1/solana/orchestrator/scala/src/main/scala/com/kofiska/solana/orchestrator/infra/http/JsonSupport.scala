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
}
