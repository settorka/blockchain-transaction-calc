package com.kofiska.solana.orchestrator.config

final case class AppConfig(
  httpHost: String,
  httpPort: Int,
  ingressMaxInFlight: Int,
  ingressRequestTimeoutMs: Long,
  maxRequestBytes: Long,
  maxRouteCandidates: Int,
  maxSourceHashes: Int,
  computeHost: String,
  computePort: Int,
  postgresUrl: String,
  postgresUser: String,
  postgresPassword: String,
  postgresPoolSize: Int,
  postgresConnectionTimeoutMs: Long,
  valkeyUri: String,
  dedupeTtlSeconds: Long,
  auditMaxAttempts: Int,
  auditRetryDelaySeconds: Long,
  auditBootstrapServers: String,
  auditTopic: String
)

object AppConfig {
  def load(): AppConfig =
    validate(
      AppConfig(
        httpHost = env("HTTP_HOST", "0.0.0.0"),
        httpPort = env("HTTP_PORT", "8080").toInt,
        ingressMaxInFlight = env("INGRESS_MAX_IN_FLIGHT", "1024").toInt,
        ingressRequestTimeoutMs = env("INGRESS_REQUEST_TIMEOUT_MS", "5000").toLong,
        maxRequestBytes = env("MAX_REQUEST_BYTES", "65536").toLong,
        maxRouteCandidates = env("MAX_ROUTE_CANDIDATES", "16").toInt,
        maxSourceHashes = env("MAX_SOURCE_HASHES", "16").toInt,
        computeHost = env("COMPUTE_HOST", "127.0.0.1"),
        computePort = env("COMPUTE_PORT", "50051").toInt,
        postgresUrl = env("POSTGRES_URL", "jdbc:postgresql://127.0.0.1:5432/decision_store"),
        postgresUser = env("POSTGRES_USER", "decision"),
        postgresPassword = env("POSTGRES_PASSWORD", "decision"),
        postgresPoolSize = env("POSTGRES_POOL_SIZE", "8").toInt,
        postgresConnectionTimeoutMs = env("POSTGRES_CONNECTION_TIMEOUT_MS", "5000").toLong,
        valkeyUri = env("VALKEY_URI", "redis://127.0.0.1:6379/0"),
        dedupeTtlSeconds = env("DEDUPE_TTL_SECONDS", "3600").toLong,
        auditMaxAttempts = env("AUDIT_MAX_ATTEMPTS", "3").toInt,
        auditRetryDelaySeconds = env("AUDIT_RETRY_DELAY_SECONDS", "30").toLong,
        auditBootstrapServers = env("AUDIT_BOOTSTRAP_SERVERS", "127.0.0.1:9092"),
        auditTopic = env("AUDIT_TOPIC", "solana.audit")
      )
    )

  private def env(name: String, default: String): String =
    sys.env.get(name).map(_.trim).filter(_.nonEmpty).getOrElse(default)

  private def validate(config: AppConfig): AppConfig = {
    require(config.httpHost.nonEmpty, "HTTP_HOST must not be empty")
    require(config.httpPort > 0, "HTTP_PORT must be positive")
    require(config.ingressMaxInFlight > 0, "INGRESS_MAX_IN_FLIGHT must be positive")
    require(config.ingressRequestTimeoutMs > 0, "INGRESS_REQUEST_TIMEOUT_MS must be positive")
    require(config.maxRequestBytes > 0, "MAX_REQUEST_BYTES must be positive")
    require(config.maxRouteCandidates > 0, "MAX_ROUTE_CANDIDATES must be positive")
    require(config.maxSourceHashes > 0, "MAX_SOURCE_HASHES must be positive")
    require(config.computeHost.nonEmpty, "COMPUTE_HOST must not be empty")
    require(config.computePort > 0, "COMPUTE_PORT must be positive")
    require(config.postgresUrl.nonEmpty, "POSTGRES_URL must not be empty")
    require(config.postgresUser.nonEmpty, "POSTGRES_USER must not be empty")
    require(config.postgresPassword.nonEmpty, "POSTGRES_PASSWORD must not be empty")
    require(config.postgresPoolSize > 0, "POSTGRES_POOL_SIZE must be positive")
    require(config.postgresConnectionTimeoutMs > 0, "POSTGRES_CONNECTION_TIMEOUT_MS must be positive")
    require(config.valkeyUri.nonEmpty, "VALKEY_URI must not be empty")
    require(config.dedupeTtlSeconds > 0, "DEDUPE_TTL_SECONDS must be positive")
    require(config.auditMaxAttempts > 0, "AUDIT_MAX_ATTEMPTS must be positive")
    require(config.auditRetryDelaySeconds > 0, "AUDIT_RETRY_DELAY_SECONDS must be positive")
    require(config.auditBootstrapServers.nonEmpty, "AUDIT_BOOTSTRAP_SERVERS must not be empty")
    require(config.auditTopic.nonEmpty, "AUDIT_TOPIC must not be empty")
    config
  }
}
