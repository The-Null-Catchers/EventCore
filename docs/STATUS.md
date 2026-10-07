# Implementation and release status

The upstream repository was empty on initial inspection. No existing architecture or implementation was replaced. The highest-priority gap was a real durable broker and producer/consumer correctness; work began there rather than a dashboard mockup.

## Evidence

| Scenario | Evidence |
|---|---|
| Segments rotate and recover sequential offsets | Go storage restart test |
| Acknowledged append survives SIGKILL without graceful Close | Subprocess crash/recovery test |
| Concurrent producers preserve contiguous offsets | 20 producers, 1,000 events, race detector |
| Torn tail repaired; complete checksum corruption rejected | Storage corruption tests |
| Retention preserves offsets across restart | Storage retention test |
| Key routing and unkeyed balancing | Broker tests + SDK acceptance script |
| Group assignment, stale-generation/lease rejection, redelivery | Coordinator tests with two members and expiring leases |
| Failed metadata commit does not advance offsets | Coordinator failure-injection test |
| Partial commit → suffix redelivery → restart; stale tokens rejected | Coordinator recovery/fencing tests, scoped HTTP tests, SDK tests; Docker acceptance exercises partial commits |
| Authentication, CSRF, scopes and workspace isolation | HTTP API tests |
| Optional schema persists and blocks invalid data/external refs | Broker schema test |
| HTTP 500 → bounded retries → durable DLQ → source commit | Webhook integration test using injected HTTP transport |
| DLQ retry/discard, concurrent resolution, ambiguous prepare, restart recovery and retention pins | Disk/outbox fault tests, HTTP authorization/audit tests, PostgreSQL integration, webhook failure → DLQ → retry → successful delivery |
| Encryption and SSRF address rules | Webhook security tests |
| SDK publication does not blindly retry ambiguous failures | JS/Python SDK tests |
| Bounded timestamp replay, unchanged source, scoped copying and partial-failure retry | Storage/API/SDK tests; Docker acceptance copies 10,001 retained events to archive |
| Concurrent duplicate publication, conflict detection, bounded index, retention and crash recovery | Disk broker race tests; SIGKILL receipt recovery; scoped API and SDK tests |
| Real PostgreSQL sessions, keys and committed offsets | Environment-gated test; executed by CI |
| Docker Compose → 10,000 SDK events → two members → rebalance → restart → replay | Dedicated GitHub Actions acceptance job |

The local environment does not provide Docker or PostgreSQL. Local unit/integration tests use real disk logs and injected auth/metadata/HTTP dependencies where named above. CI status is the source of truth for external database/container evidence; a workflow file alone is not a passing result.

## Verified external run

[CI run 37240421722](https://github.com/The-Null-Catchers/EventCore/actions/runs/37240421722) passed all three jobs on 2026-10-04 UTC at commit `9058364`: backend race/vet tests against PostgreSQL, JS/Python SDK tests and dependency audit, and Docker Compose acceptance with 10,000 initial events plus restart/reset/replay of 10,001. The earlier service-health quoting failure was fixed before this passing run. Subsequent hardening commits must pass their own CI before these results can be attributed to their exact SHA.

## Required before tagging v0.1.0

1. Complete and test a professional dashboard against these real APIs. No dashboard screenshots exist yet.
2. Durable bounded producer idempotency is implemented, with scope/age/capacity/retention boundaries documented. Exact-SHA external CI evidence remains required before release.
3. Explicit fenced processed-prefix commits are implemented alongside batch ack, with no automatic commit. Verify the new exact SHA in CI before release.
4. Bounded timestamp/range cursor replay, replay-to-topic and JSONL page export are implemented. Durable DLQ retry/discard decisions are implemented. Server-persisted replay jobs and import remain open.
5. Add WebSocket consumers and JS/Python asynchronous transport parity, with connection recovery tests.
6. Add webhook custom-header support, per-attempt historical rows (currently the latest state per event), real external endpoint testing, and terminal 4xx classification. Current retries remain bounded.
7. Add topic update/delete with confirmation and auditing, schema version/lifecycle management and collision rules for user-created `.DLQ` names.
8. Add user provisioning, role administration, workspace management and password-reset token/email flow. Membership isolation currently works, but owner provisioning is bootstrap-only.
9. Add disk-pressure warning states, broker version/git SHA/system diagnostics, OpenTelemetry, consumer-lag and delivery Prometheus metrics, and a Prometheus/Grafana deployment profile.
10. Add full OpenAPI, measured sustained/burst/slow-consumer load tests, storage growth results, SDK package/release automation, dependency scanning for Go and container images, and reproducible image digest pins.
11. Run the complete requested 16-step workflow, including a real configured HTTP-500 endpoint and dashboard accuracy checks, then publish release notes/screenshots and tag v0.1.0.

## Operational boundaries

- One broker process, one metadata database, one writable data volume. No clustering, replication or failover.
- Bounded producer idempotency (4,096 receipts/topic, 24 hours, retained log lifetime); at-least-once group and webhook semantics; no exactly-once or transactional producer guarantee.
- SQL commits and file appends are distinct durable boundaries. Publication/HTTP/DLQ uncertainty can cause duplicates.
- Group assignments use sorted member IDs and partition `id % member_count`; joins/leaves/expiry advance the generation and invalidate in-flight leases. Membership is intentionally ephemeral across broker restart, committed offsets are durable.
- API request limits are in-process, reset at restart and do not coordinate multiple nodes. Batch requests count as one request; their length and total body size are bounded separately.
- Default leases are 30 seconds. Applications must finish/ack within that time or expect redelivery. There is no explicit lease extension endpoint yet.
- Topic quotas exist; group count and PostgreSQL attempt/session history still need lifetime quotas/cleanup. Metadata health failures stop startup/readiness and prevent commits.
- SSE is an observer cursor, not a consumer-group acknowledgement channel. Filters scan a bounded page; `next_offset` reflects scanned events, not just matched events.
