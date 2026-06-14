# Gaps (v2 Go-Live)

This file lists what cannot yet be claimed as "go-live" for v2.

Spec is in [`v2/look-ahead.md`](./v2/look-ahead.md). Delivery register is [`v2/plan.md`](./v2/plan.md).

## Ingress

- No REST ingress exists yet.
- No request auth mechanism defined (even if single-operator).
- No GCP firewall IP allowlist spec wired to the service boundary.
- No ingress limits (bytes/routes/hops/deadlines/inflight) proven end-to-end.
- No overload semantics proven (fail closed, no backlog spiral).

## Unknowns

- We do not know the market adapter truth set yet.
  - Which providers exist.
  - Which fields are required for `ACCEPT`.
  - How provider conflicts are detected and handled.
  - What freshness windows mean (slot lag, wall-clock age).
- We do not know the snapshot storage shape yet.
  - Where canonical snapshot bytes live.
  - How snapshot references resolve.
  - How snapshot schema changes are handled.
- We do not know the bounds yet.
  - Max request bytes.
  - Max routes and hops.
  - Max inflight and queue age.
  - Deadlines and retry limits.
- We do not know the exact-once keys yet for REST ingress.
  - What uniqueness constraints are enforced in Postgres.
  - How duplicates behave under concurrency.
- We do not know exposure semantics yet.
  - Unit definition and windowing.
  - Atomic update semantics tied to decision commits.
- We do not know the policy bundle shape yet.
  - Format, validation, version/hash.
  - What “approved” means in v2.
- We do not know the evidence storage contract yet.
  - Which artifacts are stored vs referenced.
  - What makes an `ACCEPT` replayable from stored artifacts.
- We do not know outbox shipper poison behavior yet.
  - What happens on invalid payload, broker errors, or oversized records.
  - Whether one bad row blocks shipping.
  - Dead-letter rules and stop-accept triggers.
- We do not know the measured throughput ceiling yet.
  - End-to-end rps with Postgres decision+outbox commits and compute in the loop.
  - Tail latency drivers (GC, DB commit latency, gRPC serialization).

## Sources + Snapshots

- No market adapter exists that can build source snapshots.
  - No provider policy (which sources, commitment, freshness windows, conflict rules).
  - No deterministic snapshot serialization + hashing contract (bytes + schema version + hash).
  - No snapshot storage or reference strategy.
  - No “decision throughput decoupled from provider call rate” implementation (bounded polling/caching).
  - No “provider conflict blocks ACCEPT” enforcement.

## Exactly-Once Terminalization

- Exactly-once terminalization is not DB-enforced for the v2 ingress shape.
  - Dedupe cache (Valkey) cannot be the source of truth.
  - Postgres uniqueness + replay semantics must be defined and tested under concurrency/duplicate storms.

## Reconstructible ACCEPT

- “Reconstructible ACCEPT” is not implemented as a retrieval + replay contract.
  - Canonical normalized input bytes are not guaranteed to be stored.
  - Canonical snapshot bytes are not guaranteed to be stored and retrievable.
  - Policy bundle versioning/approval gate is not enforced (must be non-bypassable).
  - Release/version identity is not guaranteed to be attached to every decision.
  - Gate evaluation trace sufficient to justify ACCEPT is not guaranteed to exist.

## Audit Outbox

- Outbox semantics are not enforced as an `ACCEPT` prerequisite in the v2 service shape.
  - Decision + outbox must commit in the same Postgres transaction.
  - Redpanda shipping must be idempotent; lag must be measured.
  - Tamper-evidence is not defined (digest/hash chain) or explicitly disclaimed.

## Money Math

- Integer base-unit money path is not guaranteed end-to-end.
  - Float usage in money decisions must be removed or isolated to non-source-of-truth paths.
  - Overflow/precision policies per token are not defined.
  - `EV_lower_bound` uncertainty components are not calibrated (fee/slippage/source uncertainty).

## Exposure Limits

- Exposure accounting is not defined as an atomic, idempotent control.
  - Unit definition (notional, token, pair) is not fixed.
  - Windowing (rolling 1m/1h/24h) is not fixed.
  - Update semantics under retries/duplicates/replays are not defined.
  - “Cannot evaluate exposure => no ACCEPT” is not proven.

## Stop-Accept

- Stop-accept is not wired to readiness/health signals.
  - No binary “accepting enabled” gate proven.
  - No thresholds for snapshot freshness, compute deadline failures, queue age, outbox lag.
  - No operator-visible “why we stopped accepting” summary proven.

## No-Execution

- No-execution proof is not demonstrated for v2.
  - No build/startup guards proven (fail startup if execution config exists).
  - No tests proving “no execution adjacency” (no signer, no submission, no forward-to-executor).

## Proof: Load + Failures

- Behavior at `1,000,000 ops/hour` decision throughput is not proven.
  - No load test demonstrating throughput using cached snapshots (not per-request provider calls).
  - No failure injection run proving fail-closed behavior for: Postgres, Valkey, compute, snapshot failures, provider conflicts, broker outage, latency spikes.
  - No proof that hostile patterns do not cause latency spirals (duplicate storms, worst-case payloads, hot keys).
