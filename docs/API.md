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
| GET `/v1/topics/{topic}/events` | partition, offset, limit? (1..1000 scanned), end_offset?, from_time?, until_time?, type?, key?, id? | events[], next_offset, end_offset, scanned, done | read |
| GET `/v1/topics/{topic}/export` | same bounded range query as events | JSONL + cursor response headers | read |
| POST `/v1/topics/{topic}/replay` | partition, offset, end_offset, target, replay_id, confirm:true, limit? (1..100 scanned), from_time?, until_time?, type?, key?, id? | copied receipts + cursor; partial progress on failure | source read + target produce |
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
| POST `/v1/webhooks` | topic, url, secret (32..1024 bytes), headers? (encrypted), max_attempts (1..20), delay_seconds (1..3600), paused? | 201 ok; list to retrieve generated ID | admin |
| GET `/v1/webhooks` | none | subscriptions in workspace; secret/header values omitted, header_names included | admin |
| PATCH `/v1/webhooks/{id}` | paused | ok | admin |
| GET `/v1/webhooks/{id}/attempts` | none | at most100 latest per-event attempt states | admin |
| GET `/v1/webhooks/{id}/history` | after? (exclusive cursor, default0), limit? (1..100, default50) | entries, next_cursor, has_more; immutable delivery transitions | admin |

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

## Bounded replay and export

Read a fixed partition snapshot with `GET /v1/topics/orders/events?partition=0&offset=0&limit=100`. Save its `end_offset`, then keep that value on subsequent pages and advance to `next_offset` until `done:true`. The exclusive end offset excludes later appends. Filters use `from_time` inclusive and `until_time` exclusive (RFC3339), plus exact ID/type/key. Limits bound **scanned** records, so a filtered page can be empty while `done:false`; keep advancing. Timestamp order is not assumed because server clocks can change. Each page reads at most 1,000 records / approximately 4 MiB, including one final record beyond the byte threshold. Retention is not pinned: if your next offset is deleted, HTTP416 requires an explicit range decision. These are caller-owned temporary replay cursors, not server-persisted jobs.

Export uses the same query and returns `application/x-ndjson`; response headers `X-EventCore-Next-Offset`, `X-EventCore-End-Offset`, `X-EventCore-Scanned` and `X-EventCore-Done` describe the page, including empty pages. Exported events retain original IDs/timestamps and are never removed from the log. Import remains a future operation.

Copy to a different existing topic (in the same authenticated workspace):

```json
{"partition":0,"offset":0,"end_offset":1000,"limit":100,"target":"orders.archive","replay_id":"recovery-2026-10-07","confirm":true,"from_time":"2026-10-01T10:00:00Z","until_time":"2026-10-01T11:00:00Z"}
```

POST this to `/v1/topics/orders/replay`. Both source read and destination produce permissions are required; same-topic replay is rejected. The server audits the attempt before writing. Each copied event receives a new ID/offset/timestamp, preserves type/key/data and passes normal destination size/schema/disk checks. Reserved `eventcore.replay.*` headers identify run ID and original ID/topic/partition/offset; existing reserved values are replaced. If the augmented header count or event size exceeds destination limits, copying stops with a clear error. Consumer-group offsets and the source log are untouched. Existing consumers can observe the copied topic, or use inactive-group reset to replay the original log.

On success the response contains `receipts`, `next_offset`, `end_offset`, `scanned` and `done`. On partial failure the HTTP400/409/503 response also contains completed receipts, the failed source offset as `next_offset`, and `error`; no later selected event is copied. Earlier successful writes remain durable. Reuse the same `replay_id` across pages and retries: a derived idempotency key per original event prevents duplicates **only within the producer receipt age/capacity/retention window**. Large jobs or delayed retries can exceed that window; this is at-least-once copying, not transactional/exactly-once replay. A new run ID intentionally creates fresh copies. Do not automatically retry mutations after arbitrary network failures.

Python: `client.scan(topic, partition, offset, end_offset=..., from_time=...)` and `client.replay(topic, target, replay_id, partition, offset, end_offset, ...)`. TypeScript: `client.scan(topic, options)` and `client.replay(topic, {...options, target, replay_id, end_offset})`. SDK errors surface HTTP failures with the response body in `error.details` (including partial replay progress); callers may explicitly retry the fixed page/run ID within the documented deduplication boundary.

## Dead-letter decisions

`GET /v1/topics/{topic.DLQ}/dead-letters?partition=0&offset=0&limit=100` requires DLQ read scope and returns `entries` (original DLQ event, optional decision, diagnostic for malformed records), `next_offset`, `end_offset`, `scanned`, and `done`. Range/time filters match the event explorer; limits apply to scanned records. Internal outbox payloads are never returned.

`POST /v1/topics/{topic.DLQ}/dead-letters/retry` or `/discard` requires DLQ admin permission (retry also requires original-topic produce permission), CSRF for cookies, and `{"partition":0,"offset":0,"confirm":true}`. Returns the durable decision (`action`, `status`, actor, timestamps, and a new-event receipt for retry). Repeating the same action returns the saved result; a different action on an already resolved record returns 409. Audit attempts include the DLQ topic, partition and offset. Workspace comes from authentication.

Retry publishes the original type/key/data/headers to its original topic through normal schema/size validation, with a new ID/timestamp and `eventcore.dlq.*` provenance headers. All consumers of that topic can see it, including other webhooks; this is not a targeted webhook redelivery. Discard records a logical decision and preserves the append-only DLQ record until retention. Invalid/non-webhook DLQ envelopes return 400; unavailable metadata/storage returns 503; expired cursors return 416.

A pending PostgreSQL outbox row reserves the new partition offset before the broker append. An ambiguous prepare fences the partition until restart. Startup verifies/replays pending reservations before serving traffic or running workers/retention. Pending receipts pin retention; completion removes the stored candidate payload and releases the pin. This mechanism is independent of the bounded producer-idempotency cache. Keep broker data and metadata backups consistent: a missing/conflicting reserved record stops recovery rather than silently duplicating it. Decision history currently has no automatic cleanup.


## Webhook delivery policy and history

Subscriptions accept optional `headers`, for example `{"Authorization":"Bearer receiver-token","X-Client":"billing"}`. Names are validated HTTP tokens and normalized case-insensitively; duplicate names, CR/LF/non-ASCII values, routing/hop-by-hop headers, `X-Forwarded-*` and `X-EventCore-*` are rejected. Limits: 16 headers, 64-byte names, 1024-byte values, 8 KiB combined. Content-Type, User-Agent and signature fields are platform-controlled. Header values are AES-GCM encrypted with separate authenticated context from signing secrets; GET subscriptions exposes only their names. Put credentials in headers rather than URL query parameters.

Each delivery has a 10-second timeout and bounded response-body drain. HTTP 2xx is success. Redirects and permanent 4xx (except 408, 425 and 429) go to DLQ immediately. Network errors, 408/425/429 and 5xx retry with exponential delay up to one hour and the configured attempt limit. For 429/503, `Retry-After` seconds or HTTP dates can extend the delay, capped at one hour. Attempts and source ordering remain at least once.

`history` contains immutable `sending` and outcome transitions per attempt number, plus `dlq` routing. A recovered incomplete send records `unknown`: the receiver may have processed it, or the request may never have left the broker. `started_at`, `completed_at`, HTTP status, latency and sanitized diagnostics describe known outcomes; bodies, URL-derived errors and header values are never stored in history. The latest state and history transition commit together. Per-subscription SQL serialization makes cursor pagination safe from late lower-cursor commits. Repeat the cursor from `next_cursor`; `has_more=false` means no more rows at query time, and later requests can see new transitions. JS `webhookHistory(id, after, limit)` and Python `webhook_history(id, after, limit)` use this endpoint. The older `attempts` response shape stays unchanged.

On upgrade, the previously available latest state is seeded once into history. Earlier attempts cannot be reconstructed and are not invented; legacy entries may lack delivery timestamps. History currently has no automatic expiry or lifetime quota. Worker/configuration/metadata errors surface to operational logs and the caller; secrets and response bodies remain excluded.

Verify incoming webhook signatures over the **raw request bytes** before decoding JSON, using a constant-time comparison. Reject stale timestamps (for example, more than 5 minutes away), keep clocks synchronized, and deduplicate `X-EventCore-ID` in your application because valid requests can be retried:

```python
import hashlib, hmac, time

def verify(secret, timestamp, signature, raw_body):
    try:
        if abs(time.time() - int(timestamp)) > 300:
            return False
    except (ValueError, TypeError):
        return False
    expected = 'v1=' + hmac.new(secret.encode(), timestamp.encode() + b'.' + raw_body,
                               hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)
```
