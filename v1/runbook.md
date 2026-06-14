# v1 Runbook

## Endpoints

- `GET /v1/healthz`
- `GET /v1/readyz`
- `GET /v1/metrics`
- `POST /v1/decisions:preflight`

## Start order

1. Start PostgreSQL.
2. Start Valkey.
3. Start Redpanda.
4. Start Rust compute on `COMPUTE_HOST:COMPUTE_PORT`.
5. Start the Scala orchestrator.

The orchestrator binds REST ingress after startup. It uses the configured compute host, PostgreSQL pool, Valkey URI, and audit topic.

## Request lifecycle

1. REST ingress receives the request.
2. Request shape is validated.
3. Admission checks the in-flight limit.
4. Workflow claims the dedupe key.
5. Workflow calls Rust compute.
6. Workflow persists the decision and outbox row in PostgreSQL.
7. Workflow publishes the audit event.
8. Reconciliation retries pending audit rows and repairs dedupe drift.

## Steady state

- `200` means the request returned a terminal decision.
- `400` or `413` means the request failed validation.
- `429` means the in-flight limit was reached.
- `500` means workflow failure.
- `/v1/metrics` reports request counts, in-flight count, request latency, compute latency, replay hit count, replay drift count, audit backlog, and pool state.

## Recovery

- If PostgreSQL, Valkey, Redpanda, or compute fail, the request path fails.
- Replay uses durable decision rows.
- Reconciliation runs on startup and every 5 minutes.
- If audit publishing fails, the outbox row moves to `retry` or `dead`.

## Shutdown

1. Stop sending new requests.
2. Wait for in-flight requests to finish.
3. Stop the orchestrator.
4. Stop compute.
5. Stop Redpanda.
6. Stop Valkey.
7. Stop PostgreSQL.

