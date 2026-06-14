package com.kofiska.solana.orchestrator.service

import com.kofiska.solana.orchestrator.domain._

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

trait RuntimeMetrics {
  def recordIngressRequest(): Unit
  def recordIngressAccepted(): Unit
  def recordIngressRejected(): Unit
  def recordIngressOverloaded(): Unit
  def recordRequestLatencyMs(value: Long): Unit
  def recordComputeLatencyMs(value: Long): Unit
  def recordTerminalState(terminalState: TerminalState): Unit
  def recordReplayHit(): Unit
  def recordReplayDrift(): Unit
  def recordAuditPublishFailure(): Unit
  def recordAuditRetryScheduled(): Unit
  def recordAuditDeadLetter(): Unit
  def recordDedupeRepair(): Unit
  def inFlightAcquired(): Unit
  def inFlightReleased(): Unit
  def snapshot(auditBacklog: AuditBacklogSnapshot, pool: PoolSnapshot): RuntimeMetricsSnapshot
}

object RuntimeMetrics {
  val noop: RuntimeMetrics = new RuntimeMetrics {
    override def recordIngressRequest(): Unit = ()
    override def recordIngressAccepted(): Unit = ()
    override def recordIngressRejected(): Unit = ()
    override def recordIngressOverloaded(): Unit = ()
    override def recordRequestLatencyMs(value: Long): Unit = ()
    override def recordComputeLatencyMs(value: Long): Unit = ()
    override def recordTerminalState(terminalState: TerminalState): Unit = ()
    override def recordReplayHit(): Unit = ()
    override def recordReplayDrift(): Unit = ()
    override def recordAuditPublishFailure(): Unit = ()
    override def recordAuditRetryScheduled(): Unit = ()
    override def recordAuditDeadLetter(): Unit = ()
    override def recordDedupeRepair(): Unit = ()
    override def inFlightAcquired(): Unit = ()
    override def inFlightReleased(): Unit = ()
    override def snapshot(auditBacklog: AuditBacklogSnapshot, pool: PoolSnapshot): RuntimeMetricsSnapshot =
      RuntimeMetricsSnapshot(
        ingressRequests = 0L,
        ingressAccepted = 0L,
        ingressRejected = 0L,
        ingressOverloaded = 0L,
        inflightCurrent = 0,
        requestLatencyTotalMs = 0L,
        requestLatencySamples = 0L,
        requestLatencyMaxMs = 0L,
        computeLatencyTotalMs = 0L,
        computeLatencySamples = 0L,
        computeLatencyMaxMs = 0L,
        terminalAcceptCount = 0L,
        terminalDeferCount = 0L,
        terminalRejectCount = 0L,
        terminalFailedCount = 0L,
        replayHits = 0L,
        replayDrift = 0L,
        auditPublishFailures = 0L,
        auditRetryScheduled = 0L,
        auditDeadLetters = 0L,
        dedupeRepairs = 0L,
        auditBacklog = auditBacklog,
        pool = pool
      )
  }

  def live(): RuntimeMetrics = new InMemoryRuntimeMetrics
}

final class InMemoryRuntimeMetrics extends RuntimeMetrics {
  private val ingressRequests = new AtomicLong(0L)
  private val ingressAccepted = new AtomicLong(0L)
  private val ingressRejected = new AtomicLong(0L)
  private val ingressOverloaded = new AtomicLong(0L)
  private val inFlight = new AtomicInteger(0)
  private val requestLatencyTotalMs = new AtomicLong(0L)
  private val requestLatencySamples = new AtomicLong(0L)
  private val requestLatencyMaxMs = new AtomicLong(0L)
  private val computeLatencyTotalMs = new AtomicLong(0L)
  private val computeLatencySamples = new AtomicLong(0L)
  private val computeLatencyMaxMs = new AtomicLong(0L)
  private val terminalAcceptCount = new AtomicLong(0L)
  private val terminalDeferCount = new AtomicLong(0L)
  private val terminalRejectCount = new AtomicLong(0L)
  private val terminalFailedCount = new AtomicLong(0L)
  private val replayHits = new AtomicLong(0L)
  private val replayDrift = new AtomicLong(0L)
  private val auditPublishFailures = new AtomicLong(0L)
  private val auditRetryScheduled = new AtomicLong(0L)
  private val auditDeadLetters = new AtomicLong(0L)
  private val dedupeRepairs = new AtomicLong(0L)

  override def recordIngressRequest(): Unit =
    ingressRequests.incrementAndGet()

  override def recordIngressAccepted(): Unit =
    ingressAccepted.incrementAndGet()

  override def recordIngressRejected(): Unit =
    ingressRejected.incrementAndGet()

  override def recordIngressOverloaded(): Unit =
    ingressOverloaded.incrementAndGet()

  override def recordRequestLatencyMs(value: Long): Unit =
    updateTimer(requestLatencyTotalMs, requestLatencySamples, requestLatencyMaxMs, value)

  override def recordComputeLatencyMs(value: Long): Unit =
    updateTimer(computeLatencyTotalMs, computeLatencySamples, computeLatencyMaxMs, value)

  override def recordTerminalState(terminalState: TerminalState): Unit =
    terminalState match {
      case TerminalState.Accept => terminalAcceptCount.incrementAndGet()
      case TerminalState.Defer  => terminalDeferCount.incrementAndGet()
      case TerminalState.Reject => terminalRejectCount.incrementAndGet()
      case TerminalState.Failed => terminalFailedCount.incrementAndGet()
    }

  override def recordReplayHit(): Unit =
    replayHits.incrementAndGet()

  override def recordReplayDrift(): Unit =
    replayDrift.incrementAndGet()

  override def recordAuditPublishFailure(): Unit =
    auditPublishFailures.incrementAndGet()

  override def recordAuditRetryScheduled(): Unit =
    auditRetryScheduled.incrementAndGet()

  override def recordAuditDeadLetter(): Unit =
    auditDeadLetters.incrementAndGet()

  override def recordDedupeRepair(): Unit =
    dedupeRepairs.incrementAndGet()

  override def inFlightAcquired(): Unit =
    inFlight.incrementAndGet()

  override def inFlightReleased(): Unit =
    inFlight.decrementAndGet()

  override def snapshot(auditBacklog: AuditBacklogSnapshot, pool: PoolSnapshot): RuntimeMetricsSnapshot =
    RuntimeMetricsSnapshot(
      ingressRequests = ingressRequests.get(),
      ingressAccepted = ingressAccepted.get(),
      ingressRejected = ingressRejected.get(),
      ingressOverloaded = ingressOverloaded.get(),
      inflightCurrent = inFlight.get(),
      requestLatencyTotalMs = requestLatencyTotalMs.get(),
      requestLatencySamples = requestLatencySamples.get(),
      requestLatencyMaxMs = requestLatencyMaxMs.get(),
      computeLatencyTotalMs = computeLatencyTotalMs.get(),
      computeLatencySamples = computeLatencySamples.get(),
      computeLatencyMaxMs = computeLatencyMaxMs.get(),
      terminalAcceptCount = terminalAcceptCount.get(),
      terminalDeferCount = terminalDeferCount.get(),
      terminalRejectCount = terminalRejectCount.get(),
      terminalFailedCount = terminalFailedCount.get(),
      replayHits = replayHits.get(),
      replayDrift = replayDrift.get(),
      auditPublishFailures = auditPublishFailures.get(),
      auditRetryScheduled = auditRetryScheduled.get(),
      auditDeadLetters = auditDeadLetters.get(),
      dedupeRepairs = dedupeRepairs.get(),
      auditBacklog = auditBacklog,
      pool = pool
    )

  private def updateTimer(total: AtomicLong, samples: AtomicLong, max: AtomicLong, value: Long): Unit = {
    total.addAndGet(value)
    samples.incrementAndGet()
    var current = max.get()
    while (value > current && !max.compareAndSet(current, value)) {
      current = max.get()
    }
  }
}
