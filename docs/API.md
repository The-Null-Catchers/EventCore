# HTTP API v1

Base URL in local Compose: `http://localhost:8080`. JSON envelopes use server-assigned ID/topic/partition/offset/UTC timestamp and producer type/key/headers/data and optional idempotency_key. Server version: `0.1.0-dev`. Topic and group names must pass the constrained name rule in STORAGE.md. Partition offsets and committed offsets are signed 64-bit nonnegative integers; SDKs use JSON numbers, so JavaScript offsets above `Number.MAX_SAFE_INTEGER` are not supported yet.

All routes except health/readiness/login require `Authorization: Bearer <api-key>` or the `eventcore_session` cookie. Cookie mutations also require `X-CSRF-Token`. Permissions are workspace-bound. Role owner/admin permits all operations, developer permits produce/consume/read, viewer permits read. API keys explicitly grant `topic:<name>:produce|consume|read`, wildcard topic equivalents, or `admin`.

| Method and path | Body / query | Response | Permission |
|---|---|---|---|
| GET `/health` | none | process status/version | public |
| GET `/ready` | none | 200 or 503; checks DB and writable storage | public |
| GET `/metrics` | none | Prometheus text for current workspace | admin |
| POST `/v1/auth/login` | email, password, workspace | CSRF token and HttpOnly cookie | credentials |
| GET `/v1/auth/me` | none | ID, workspace, role, CSRF token | authenticated |
| POST `/v1/auth/logout` | none | ok; removes session | session + CSRF |
| POST `/v1/keys` | name, scopes[], expires_at (RFC3339, within one year) | id, raw token shown once | admin |
| DELETE `/v1/keys/{id}` | none | ok; workspace-constrained revocation | admin |
| GET `/v1/topics` | none | topics visible to read scope | read |
| POST `/v1/topics` | name, partitions, optional description, max_event_bytes, retention_bytes, retention_seconds, schema | 201 ok | admin |
| GET `/v1/topics/{topic}` | none | topic + partition bounds | read |
| POST `/v1/topics/{topic}/events` | type, key?, headers?, data | 201 complete stored event | produce |
| POST `/v1/topics/{topic}/events/batch` | events[], 1..100 | results[] with event OR error | produce |
| GET `/v1/topics/{topic}/events` | partition, offset, limit? (1..1000), type?, key? | events[], next_offset | read |
| GET `/v1/topics/{topic}/stream` | partition, offset? (default latest), type?; Last-Event-ID header | bounded SSE events and heartbeats | read |
| POST `/v1/topics/{topic}/groups` | name, start: earliest or latest | 201 ok | admin |
| GET `/v1/topics/{topic}/groups/{group}` | none | epoch, offsets, members, assignments, lag | consume |
| POST `/v1/topics/{topic}/groups/{group}/join` | member | current snapshot/epoch | consume |
| POST `/v1/topics/{topic}/groups/{group}/leave` | member | ok; invalidates outstanding generation | consume |
| POST `/v1/topics/{topic}/groups/{group}/pull` | member, epoch, limit? (default100) | partition deliveries with events[], token, epoch | consume |
| POST `/v1/topics/{topic}/groups/{group}/ack` | member, epoch, partition, token | ok; durably commits next offset | consume |
| POST `/v1/topics/{topic}/groups/{group}/commit` | member, epoch, partition, token, next_offset | ok; commits a processed prefix, releases lease | consume |
| POST `/v1/topics/{topic}/groups/{group}/nack` | same as ack | ok; leaves committed offset unchanged | consume |
| POST `/v1/topics/{topic}/groups/{group}/reset` | partition, offset, confirm:true | ok; rejects live members | admin |
| POST `/v1/webhooks` | topic, url, secret (>=32 chars), max_attempts (1..20), delay_seconds (1..3600), paused? | 201 ok; list to retrieve generated ID | admin |
| GET `/v1/webhooks` | none | subscriptions in workspace; secrets omitted | admin |
| PATCH `/v1/webhooks/{id}` | paused | ok | admin |
| GET `/v1/webhooks/{id}/attempts` | none | at most100 latest per-event attempt states | admin |

## Publish

```http
POST /v1/topics/orders/events
Authorization: Bearer <producer-key>
Content-Type: application/json

{"type":"order.created","key":"customer_123","headers":{"source":"checkout"},"data":{"order_id":"123"}}
```

Response example (values assigned by broker):

```json
{"id":"generated-random-id","topic":"orders","partition":2,"offset":10842,"timestamp":"2026-10-04T22:00:00Z","type":"order.created","key":"customer_123","headers":{"source":"checkout"},"data":{"order_id":"123"}}
```

Partitions use FNV-1a over UTF-8 key bytes modulo immutable partition count. Events without keys use per-process round-robin (the round-robin counter may restart at zero after recovery). Offsets order events within a partition; there is no total topic ordering. A batch is ordered but **not atomic**, and each result must be checked. Successfully appended events are not rolled back because a later input is invalid. The HTTP body cap applies to the entire batch.

## Consume and commit

1. Create a group with earliest/latest. That start choice applies when the group is created, not on every join.
2. Join with a unique member ID and store the returned epoch.
3. Pull with that member/epoch; each returned partition batch has a lease token.
4. Finish all events in a batch, then ack its token. To save a processed prefix, POST `commit` with `next_offset` equal to the last processed event offset plus one (Python `delivery.commit(next_offset)` / JS `delivery.commit(nextOffset)`). It must be greater than the currently committed offset and no greater than this batch’s last offset plus one. On failure nack; on no ack the lease expires.
   A successful partial commit releases the entire lease; pull again to receive the remaining suffix with a new token. The old token cannot ack, nack or commit again. Offset persistence failure preserves both the old offset and lease. Membership/generation changes and expired leases return HTTP 409. A missing or invalid next_offset returns HTTP 400. This operation cannot skip beyond delivered events or rewind; use administrative reset for replay.
5. Rejoin on HTTP409; older delivery tokens cannot move the group offset after ownership changes.
6. Pull regularly to renew membership (default TTL30s). There is no per-event ack or explicit lease extension yet. Leave on shutdown.

Reset is an administrative replay operation. Stop all members, inspect retained bounds, and set a partition's **next** offset with `confirm:true`. If retention has already removed the requested data, the API returns416. Inspection/SSE never changes committed group offsets.

## Errors and limits

Errors are JSON `{"error":"diagnostic"}`. HTTP401 indicates failed authentication/CSRF, 403 denied scope, 404 unavailable resource, 409 stale consumer generation/lease, 416 a cursor outside the retained range, 429 pre-execution rate rejection, and 503 essential infrastructure failure. HTTP400 covers malformed bodies, topic/event validation and invalid operations. Broker write/fsync and disk-pressure rejection return 503 for single publication; batch entries report their individual failures.

Mutation retries are not automatically safe: a network failure can follow a successful append, commit or webhook delivery. HTTP429 rejection happens before mutation and includes Retry-After for request limits. Producers may supply a nonempty `idempotency_key` (maximum 256 UTF-8 bytes) in each event envelope, including batch entries. It is scoped to workspace + topic, independent of partition or producer credentials. Repeating the same type/key/headers/data returns the original ID, partition, offset and timestamp with `deduplicated:true`, without appending. Reusing an indexed key for different content returns HTTP409; a batch entry contains `error` and `status:409` and later entries still execute. JSON object order/whitespace and absent versus empty headers are normalized for comparison; numeric literal spellings remain significant. The key is visible in the persisted event envelope and must not contain secrets.

The receipt index holds at most the newest 4,096 idempotent events per topic ordered by server timestamp/ID, expires them after 24 hours, and is rebuilt from retained checksummed records on restart (including after an ambiguous append). Retention, age expiry or capacity eviction ends deduplication for that key; subsequent reuse can append again. No unbounded or exactly-once guarantee is claimed. Keep server clocks synchronized. Non-idempotent publications remain unchanged. SDK publication remains single-attempt: callers may explicitly retry with the same key within these boundaries. A retry during partition fencing is rejected until recovery; a retained receipt can be returned under disk admission pressure because it does not write. Successful HTTP status stays 201 for new and deduplicated events; publication counters count only new appends. Session tokens, API key values and webhook signing secrets must never be logged.

SSE emits `id: <offset>` followed by `data: <JSON envelope>`, and `: heartbeat` comments when idle. Select one partition per connection. Last-Event-ID resumes at the following offset. Streams are observers with no group ack semantics and bounded write deadlines; reconnect and resume after a slow-client disconnect. Authenticated SSE access is rechecked periodically (approximately every 30–40 seconds) so expired/revoked keys do not retain an indefinite stream. WebSocket consumers and full machine-readable OpenAPI are pending.
