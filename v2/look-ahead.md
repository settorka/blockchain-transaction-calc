# v2 Look Ahead (Contract)

v2 is a live, non-executing Solana swap preflight decision service.

It answers one question:

```text
Given a proposed Solana swap, should it be ACCEPT, DEFER, REJECT, or FAILED_CLOSED?
```

It must use real Solana/market context, and it must be structurally unable to move funds.

## Plane Model

v2 is a plane-separated microservice deployment in a GCP VPC.

- Entry plane: REST ingress (Scala control plane).
- Control plane: Scala (Akka) owns lifecycle, policy, dedupe/replay, persistence, outbox, reconciliation, stop-accept.
- Compute plane: Rust owns bounded numeric compute behind internal gRPC.
- Data plane: Postgres is the durability boundary; Valkey is a cache acceleration only.
- Audit plane: Redpanda is downstream distribution; outbox in Postgres is authoritative evidence.

## Entry Boundary (Explicit)

- v2 MUST expose a REST ingress for requests.
- Access MUST be gated by a GCP firewall IP allowlist to your PC (no broad public access).
- The service MUST enforce request authentication and rate/burst limits even behind the allowlist.
- Internal Scala->Rust MUST remain gRPC with strict deadlines and cancellation.

## Out Of Scope (Non-Negotiable)

v2 MUST NOT include:

- signer, private keys, custody
- transaction construction or submission
- any “forward-to-executor” or execution-adjacent mode

v3 is where execution and monetized business maturity can be considered.

## Hard Invariants (Must / Must Not)

Request identity and terminalization:

- Every request MUST carry `request_id` and `dedupe_key`.
- Exactly-once terminalization MUST be enforced by durable uniqueness (database), not cache best-effort.
- Every logical request MUST end in exactly one terminal outcome.
- Duplicate requests MUST replay durable truth (return the recorded terminal decision), never recompute as the source of truth.

Durability and audit evidence:

- The system MUST persist the terminal decision before acknowledging it.
- The system MUST write an audit outbox row in the same Postgres transaction as the terminal decision.
- `ACCEPT` MUST NOT be returned unless the decision+outbox transaction commits.
- Redpanda availability MUST NOT be a synchronous prerequisite for `ACCEPT` (outbox shipping is asynchronous and measurable).

Bounds:

- Ingress MUST enforce hard numeric limits (request bytes, routes, hops, inflight, queue depth, retries, request lifetime).
- Rust compute MUST enforce hard numeric limits (routes, hops, numeric ranges) and MUST run under a strict deadline.
- Overload MUST fail closed fast (`FAILED_CLOSED`), never silently backlog until OOM.

No execution:

- v2 MUST be structurally unable to execute swaps: no signer, no tx submission path, no keys, no execution libraries.
- Startup MUST fail if any execution configuration is present.
- Tests MUST prove there is no execution or execution-adjacent path.

Reconstructible `ACCEPT` (definition, not vibes):

- For every `ACCEPT`, the system MUST be able to retrieve:
  - canonical normalized input bytes
  - canonical source snapshot bytes (or an immutable pointer to stored bytes)
  - deterministic `source_snapshot_hash` for those bytes
  - exact policy bundle version (limits + supported lists + gate thresholds)
  - exact model/config version
  - exact release/version identifier
  - gate evaluation trace sufficient to justify the decision
- If any required artifact is missing, stale, ambiguous, or conflict-ridden, `ACCEPT` is illegal.

## Source-Of-Truth Policy (Market Adapter)

The caller MUST NOT be trusted to provide truth.

- The market adapter MUST build a canonical source snapshot from explicitly defined providers.
- Decision throughput MUST be decoupled from provider call rate:
  - snapshots are polled/cached asynchronously under bounds
  - per-request RPC/quote fetching is forbidden for go-live
- Provider conflict MUST be detected and MUST block `ACCEPT` (fail closed or defer; policy-defined, but never “pick one silently”).

Required snapshot contents (minimum):

- token mints, decimals, and supported-token status
- amount in integer base units
- quote/route candidates and provider identity
- program/venue identity (allowed list applies)
- fee context, liquidity signal, confidence, timestamp
- Solana slot and quote age (slot + wall-clock)
- canonical snapshot bytes + snapshot schema version + deterministic snapshot hash

## `ACCEPT` Rule (Gate Contract)

`ACCEPT` is allowed only when every gate evaluates TRUE under the active approved policy bundle:

```text
EV_lower_bound > operating_cost
              + fee_uncertainty
              + slippage_uncertainty
              + source_uncertainty
              + safety_margin
```

And:

```text
source_snapshot_complete = true
source_snapshot_fresh = true
source_snapshot_conflict_free = true
token_supported = true
program_allowed = true
route_supported = true
liquidity_confidence >= minimum
risk_score <= limit
exposure <= limit
model_version = approved_and_active
decision_not_expired = true
persistence_writable = true
outbox_writable = true
compute_within_budget = true
```

Unknown, missing, stale, or untrusted inputs MUST NOT produce `ACCEPT`.

## Stop-Accept Conditions (Fail Closed)

Stop emitting new `ACCEPT` decisions when any is true:

- Postgres is not writable (decision/outbox commit fails).
- Market adapter cannot build fresh, complete, conflict-free snapshots.
- Rust compute is unavailable or deadline failures breach threshold.
- Queue age exceeds bound or inflight exceeds bound.
- Dedupe drift is detected (terminal mismatch between cache and durable truth).
- Exposure accounting cannot be evaluated or is at/over limit.
- Policy bundle or model/config version is unknown, unapproved, or inconsistent.

## Evidence (Required Fields)

Every terminal decision MUST carry (at minimum):

- `trace_id`
- `request_id`
- `dedupe_key`
- `decision_id`
- `terminal_state`
- `reason_code`
- `schema_version`
- `release_version`
- `policy_bundle_version`
- `model_version`
- `source_snapshot_schema_version`
- `source_snapshot_hash`
- `slot`
- `quote_age`
- `token_in`
- `token_out`
- `amount_base_units`
- `selected_route`
- `EV_estimate`
- `EV_lower_bound`
- `risk_score`
- `exposure_used`
- `exposure_limit`
- `decision_expires_at`

## Go-Live Proof (Minimum)

v2 is not “live” until each is proven end-to-end:

1. REST ingress is deployed behind a firewall IP allowlist and enforces auth + bounds + deadlines.
2. Canonical snapshots exist (bytes + schema + deterministic hash) and are referenced by decisions.
3. Exactly-once terminalization is DB-enforced and duplicate replay returns durable truth.
4. `ACCEPT` implies a committed decision+outbox transaction in Postgres.
5. Outbox shipping to Redpanda is idempotent; lag is measured; stop-accept threshold exists.
6. No-execution proof holds (build/startup/test guards).
7. Load + failure tests demonstrate bounded behavior at `1,000,000 ops/hour` decision throughput using cached snapshots (not per-request RPC).
