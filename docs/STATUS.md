# Implementation and release status

The upstream repository was empty on initial inspection. No existing architecture or implementation was replaced. The highest-priority gap was a real durable broker and producer/consumer correctness; work began there rather than a dashboard mockup.

## Evidence

| Scenario | Evidence |
|---|---|
| Segments rotate and recover sequential offsets | Go storage restart test |
| Concurrent producers preserve contiguous offsets | 20 producers, 1,000 events, race detector |
| Torn tail repaired; complete checksum corruption rejected | Storage corruption tests |
| Retention preserves offsets across restart | Storage retention test |
| Key routing and unkeyed balancing | Broker tests + SDK acceptance script |
| Group assignment, stale-generation/lease rejection, redelivery | Coordinator tests with two members and expiring leases |
| Failed metadata commit does not advance offsets | Coordinator failure-injection test |
| Authentication, CSRF, scopes and workspace isolation | HTTP API tests |
| Optional schema persists and blocks invalid data/external refs | Broker schema test |
| HTTP 500 → bounded retries → durable DLQ → source commit | Webhook integration test using injected HTTP transport |
| Encryption and SSRF address rules | Webhook security tests |
| SDK publication does not blindly retry ambiguous failures | JS/Python SDK tests |
| Real PostgreSQL sessions, keys and committed offsets | Environment-gated test; executed by CI |
| Docker Compose → 10,000 SDK events → two members → rebalance → restart → replay | Dedicated GitHub Actions acceptance job |

The local environment does not provide Docker or PostgreSQL. Local unit/integration tests use real disk logs and injected auth/metadata/HTTP dependencies where named above. CI status is the source of truth for external database/container evidence; a workflow file alone is not a passing result.

## Required before tagging v0.1.0

1. Complete and test a professional dashboard against these real APIs. No dashboard screenshots exist yet.
2. Add producer idempotency with documented recovery and deduplication boundaries.
3. Add explicit manual offset commit operations alongside the current fenced batch ack protocol; auto commit should remain opt-in.
4. Add timestamp/range replay sessions and replay-to-topic operations, bounded export/import, and DLQ retry/discard operations.
5. Add WebSocket consumers and JS/Python asynchronous transport parity, with connection recovery tests.
6. Add webhook custom-header support, per-attempt historical rows (currently the latest state per event), real external endpoint testing, and terminal 4xx classification. Current retries remain bounded.
7. Add topic update/delete with confirmation and auditing, schema version/lifecycle management and collision rules for user-created `.DLQ` names.
8. Add user provisioning, role administration, workspace management and password-reset token/email flow. Membership isolation currently works, but owner provisioning is bootstrap-only.
9. Add disk-pressure warning states, broker version/git SHA/system diagnostics, OpenTelemetry, consumer-lag and delivery Prometheus metrics, and a Prometheus/Grafana deployment profile.
10. Add full OpenAPI, measured sustained/burst/slow-consumer load tests, storage growth results, SDK package/release automation, dependency scanning for Go and container images, and reproducible image digest pins.
11. Run the complete requested 16-step workflow, including a real configured HTTP-500 endpoint and dashboard accuracy checks, then publish release notes/screenshots and tag v0.1.0.

## Operational boundaries

- One broker process, one metadata database, one writable data volume. No clustering, replication or failover.
- At-least-once group and webhook semantics; no exactly-once or transactional producer guarantee.
- SQL commits and file appends are distinct durable boundaries. Publication/HTTP/DLQ uncertainty can cause duplicates.
- Group assignments use sorted member IDs and partition `id % member_count`; joins/leaves/expiry advance the generation and invalidate in-flight leases. Membership is intentionally ephemeral across broker restart, committed offsets are durable.
- API request limits are in-process, reset at restart and do not coordinate multiple nodes. Batch requests count as one request; their length and total body size are bounded separately.
- Default leases are 30 seconds. Applications must finish/ack within that time or expect redelivery. There is no explicit lease extension endpoint yet.
- Topic quotas exist; group count and PostgreSQL attempt/session history still need lifetime quotas/cleanup. Metadata health failures stop startup/readiness and prevent commits.
- SSE is an observer cursor, not a consumer-group acknowledgement channel. Filters scan a bounded page; `next_offset` reflects scanned events, not just matched events.
