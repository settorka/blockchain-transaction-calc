package com.kofiska.solana.orchestrator

import akka.actor.typed.ActorSystem
import akka.actor.typed.scaladsl.Behaviors
import com.kofiska.solana.orchestrator.config.AppConfig
import com.kofiska.solana.orchestrator.infra.http.IngressHttpServer
import com.kofiska.solana.orchestrator.infra.grpc.GrpcComputeGateway
import com.kofiska.solana.orchestrator.infra.postgres.JdbcDecisionRepository
import com.kofiska.solana.orchestrator.infra.redpanda.KafkaAuditPublisher
import com.kofiska.solana.orchestrator.infra.valkey.ValkeyDedupeCache
import com.kofiska.solana.orchestrator.service.{ReconciliationService, RequestWorkflow}

import io.grpc.ManagedChannelBuilder

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.Await
import scala.concurrent.duration._
import akka.actor.typed.scaladsl.adapter._

object OrchestratorMain {
  def main(args: Array[String]): Unit = {
    val config = AppConfig.load()
    val channel = ManagedChannelBuilder
      .forAddress(config.computeHost, config.computePort)
      .usePlaintext()
      .build()
    val ioExecutor = Executors.newFixedThreadPool(math.max(4, config.postgresPoolSize + 4))
    implicit val ioEc: ExecutionContext = ExecutionContext.fromExecutorService(ioExecutor)
    val appEc: ExecutionContext = ExecutionContext.global

    try {
      val decisionRepository = new JdbcDecisionRepository(
        config.postgresUrl,
        config.postgresUser,
        config.postgresPassword,
        config.postgresPoolSize,
        config.postgresConnectionTimeoutMs
      )(ioEc)
      val dedupeCache = new ValkeyDedupeCache(config.valkeyUri)(ioEc)
      val auditPublisher = new KafkaAuditPublisher(config.auditBootstrapServers, config.auditTopic)(ioEc)

      val workflow = new RequestWorkflow(
        computeGateway = new GrpcComputeGateway(channel),
        decisionRepository = decisionRepository,
        dedupeCache = dedupeCache,
        auditPublisher = auditPublisher,
        dedupeTtlSeconds = config.dedupeTtlSeconds,
        auditMaxAttempts = config.auditMaxAttempts,
        auditRetryDelaySeconds = config.auditRetryDelaySeconds
      )(appEc)
      val reconciliationService = new ReconciliationService(
        decisionRepository = decisionRepository,
        dedupeCache = dedupeCache,
        auditPublisher = auditPublisher,
        dedupeTtlSeconds = config.dedupeTtlSeconds,
        auditMaxAttempts = config.auditMaxAttempts,
        auditRetryDelaySeconds = config.auditRetryDelaySeconds
      )(appEc)

      val system = ActorSystem(Behaviors.empty[Unit], "solana-orchestrator")
      val binding = Await.result(
        IngressHttpServer.start(system.toClassic, workflow, config)(appEc),
        30.seconds
      )
      system.log.info("orchestrator started")
      system.log.info("http ingress bound at {}:{}", config.httpHost, Int.box(config.httpPort))
      reconciliationService.runOnce(100)
      val reconciliationTask = system.scheduler.scheduleAtFixedRate(5.minutes, 5.minutes)(
        new Runnable {
          override def run(): Unit =
            reconciliationService.runOnce(100)
        }
      )(system.executionContext)

      sys.addShutdownHook {
        reconciliationTask.cancel()
        Await.result(binding.unbind(), 10.seconds)
        channel.shutdownNow()
        auditPublisher.close()
        dedupeCache.close()
        decisionRepository.close()
        ioExecutor.shutdown()
        system.terminate()
      }

      Await.result(system.whenTerminated, Duration.Inf)
    } finally {
      if (!channel.isShutdown) {
        channel.shutdownNow()
      }
      ioExecutor.shutdown()
    }
  }
}
