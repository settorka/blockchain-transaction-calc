# Blockchain Transaction Calculator

## Purpose

This repository evolves a local transaction calculator into a Solana swap decision system.

The system decides whether a swap request should be `accept`, `defer`, `reject`, or `fail_closed`. It is a pre-trade decision layer, not a swap executor.

Core ownership:

- Scala owns request lifecycle, state, policy, replay, persistence, and audit.
- Rust owns heavy Solana compute for quote, route, slippage, fee, breakeven, EV, freshness, and risk.
- gRPC owns the typed boundary between Scala orchestration and Rust compute.

## Architecture Principles

- Every request must have a typed identity and dedupe key.
- Every request must reach exactly one terminal outcome.
- Duplicate requests must replay from durable truth, not recompute.
- Accept requires positive `EV_lower_bound`.
- Stale, conflicting, oversized, or malformed inputs must not produce accept decisions.
- Rust offload is valid only when saved compute exceeds boundary, validation, retry, and failure cost.
- Terminal decisions must be persisted before replay can rely on them.
- Audit coverage must exist for every terminal request.

## v0: Local Scala Prototype

Status: local computation proof.

Scope:

- In-process Scala ledger demo.
- Immutable `Map[String, Double]` balance updates.
- Basic add/subtract validation.
- Local unit tests for balance behavior.
- Futures-based concurrency sketch.

Non-goals:

- no request envelope
- no durable identity
- no dedupe contract
- no replay safety
- no bounded queues
- no actor partitioning
- no persistence
- no audit trail
- no production throughput claim

Conclusion: `v0` proves local arithmetic only. It is not the operational architecture.

## v1: Bounded Risk System

Status: bounded Solana swap-preflight decision service.

Target posture: correctness under controlled load.

Request shape:

- `request_id`
- `dedupe_key`
- token pair
- amount
- route candidates
- slot and quote freshness
- source hashes
- model version

Lifecycle:

```text
ingress -> normalize -> dedupe -> classify -> queue -> gRPC compute
-> persist decision -> publish audit -> complete/replay
```

Scala orchestrator responsibilities:

- typed lifecycle control
- Akka actor orchestration
- dedupe lookup
- retry and failure policy
- gRPC dispatch
- decision persistence
- audit publishing
- reconciliation

Rust compute responsibilities:

- quote evaluation
- route scoring
- slippage cost
- fee cost
- breakeven margin
- `EV_estimate`
- `EV_lower_bound`
- freshness checks
- source checks
- risk scoring

Production-shaped dependencies:

- PostgreSQL: durable terminal decision source
- Valkey: hot-path dedupe/cache
- Redpanda: append-only audit stream
- Docker Compose: local full-stack deployment

gRPC decision:

- required for a typed Scala/Rust boundary
- required for measurable serialization and transport cost
- required for deadlines and transport failure handling
- required for independent scaling of Rust compute
- required for containerized service boundaries

v1 target: `1,000,000 ops/hour` as a budget, equal to about `277.78 ops/sec`.

v1 does not claim mainnet economic proof. It proves bounded decision behavior, durable replay, auditability, and positive lower-bound EV gating under controlled benchmark conditions.

## v2: Distributed Operating Edge

Status: planned distributed system target.

Target posture: make the `1,000,000 ops/hour` budget operationally credible.

Design:

- Scala Akka actors orchestrate distributed request flow.
- Actor partitioning is keyed by request, market, or other consistency boundary.
- Heavy Solana compute remains offloaded to Rust.
- Rust compute scales independently from Scala orchestration.

Required controls:

- bounded actor mailboxes
- bounded queues
- hot-key isolation
- worker-pool sizing
- per-stage latency budgets
- retry amplification limits
- freshness-window enforcement
- model-version compatibility checks
- replay across restart and redeploy
- cost per decision
- cost per accepted decision

Health model:

- SLIs: latency, error rate, queue depth, audit lag, replay drift, dedupe hit rate, freshness failure rate, compute saturation
- SLOs: decision latency, terminal coverage, duplicate suppression, audit coverage, replay correctness
- SLAs: only after SLOs are proven under representative load

Observability:

- OpenTelemetry across Scala, gRPC, Rust, PostgreSQL, Valkey, and Redpanda
- Jaeger for distributed request tracing
- per-stage latency attribution
- cross-service failure attribution

v2 success requires accepted requests to clear lower-bound EV, operating cost, freshness, risk, source integrity, replay, and observability controls at target throughput.

## v3: Solana Mainnet Target

Status: planned mainnet-readiness target.

Target posture: survive real Solana chain and market conditions.

Mainnet risks:

- adversarial liquidity
- slot drift
- priority-fee volatility
- account contention
- validator behavior variance
- RPC instability
- transaction expiry
- fast-changing pool state
- quote-to-execution drift

Required Solana controls:

- slot-aware freshness
- blockhash lifetime enforcement
- transaction validity windows
- priority fee strategy
- compute-unit pricing strategy
- RPC quorum and fallback
- source integrity checks
- writable account hot-spot detection
- pool-state drift detection
- route validity checks
- preflight and simulation policy
- post-submit reconciliation
- landed, failed, dropped, and expired transaction classification
- wallet, signer, and key-management boundaries
- incident response and circuit breakers

Required realized-outcome tracking:

- `EV_estimate`
- `EV_lower_bound`
- submitted transaction metadata
- landed or failed transaction status
- final slot
- confirmation depth
- realized output amount
- realized fees
- realized priority fees
- realized slippage
- `EV_realized`

v3 must close the loop between decision EV and settled Solana outcome. The system must stop accepting when RPC confidence drops, source hashes conflict, slot lag grows, route validity decays, audit coverage breaks, fees exceed limits, or realized EV diverges from the model.

## Version Summary

- `v0`: local Scala arithmetic proof.
- `v1`: bounded Scala/Rust/gRPC decision system with durable replay and audit.
- `v2`: distributed Akka-led orchestration with independently scalable Rust compute, OTEL, Jaeger, SLI/SLO/SLAs defined , and `1M ops/hour` operating target.
- `v3`: Solana mainnet target with chain-aware execution, settlement, fee, routing, freshness, and realized-EV controls.
