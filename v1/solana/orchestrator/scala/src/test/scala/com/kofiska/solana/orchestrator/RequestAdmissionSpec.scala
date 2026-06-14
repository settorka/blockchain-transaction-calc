package com.kofiska.solana.orchestrator

import com.kofiska.solana.orchestrator.domain._
import com.kofiska.solana.orchestrator.service.{IngressBounds, RequestAdmission}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

final class RequestAdmissionSpec extends AnyFlatSpec with Matchers {
  private val bounds = IngressBounds(
    maxRequestBytes = 256,
    maxRouteCandidates = 2,
    maxSourceHashes = 2
  )

  private def request(routeCandidates: Vector[IngressRouteCandidate] = defaultRoutes, sourceHashes: Vector[String] = Vector("hash-a", "hash-b")): IngressRequest =
    IngressRequest(
      requestId = "req-1",
      dedupeKey = "dedupe-1",
      traceId = "trace-1",
      modelVersion = "v1",
      tokenIn = "USDC",
      tokenOut = "SOL",
      amountIn = "1000",
      routeId = Some("route-a"),
      slot = 100,
      quoteAge = 1,
      sourceHashes = sourceHashes,
      routeCandidates = routeCandidates
    )

  private val defaultRoutes = Vector(
    IngressRouteCandidate("route-a", "direct", 1),
    IngressRouteCandidate("route-b", "aggregator", 4)
  )

  it should "accept a well formed request" in {
    val result = RequestAdmission.validate(request(), bounds)
    result.isRight shouldBe true
  }

  it should "reject too many route candidates" in {
    val result = RequestAdmission.validate(request(routeCandidates = defaultRoutes :+ IngressRouteCandidate("route-c", "amm", 2)), bounds)
    result.isLeft shouldBe true
    val error = result.swap.toOption.get
    error.reasonCode shouldBe "TOO_MANY_ROUTE_CANDIDATES"
  }

  it should "reject too many source hashes" in {
    val result = RequestAdmission.validate(request(sourceHashes = Vector("hash-a", "hash-b", "hash-c")), bounds)
    result.isLeft shouldBe true
    val error = result.swap.toOption.get
    error.reasonCode shouldBe "TOO_MANY_SOURCE_HASHES"
  }

  it should "reject invalid hop counts" in {
    val result = RequestAdmission.validate(request(routeCandidates = Vector(IngressRouteCandidate("route-a", "direct", 0))), bounds)
    result.isLeft shouldBe true
    val error = result.swap.toOption.get
    error.reasonCode shouldBe "INVALID_ROUTE_CANDIDATE"
  }
}
