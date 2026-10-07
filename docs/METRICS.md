# Operational metrics

`GET /metrics` accepts an owner/admin session or a workspace API key with `metrics:read`. This scope grants no topic, event, key-management or mutation permissions. Workspace comes from authentication; query parameters cannot select another workspace. Use one Prometheus scrape target/key per workspace and attach a consistent external `workspace` label to distinguish aggregate series. The filesystem and uptime gauges describe the shared node, not other workspaces' event volumes.

Create a key with `scopes:["metrics:read"]`, a name and a future `expires_at` through the existing administrative key endpoint. Store its once-shown raw token outside source control in a credentials file. [prometheus.yml](../infra/monitoring/prometheus.yml) is a scrape configuration example for a Prometheus process attached to the Compose network; mount the token at `/run/secrets/eventcore_metrics_token`. This file does not deploy Prometheus or Grafana. Restrict access to the monitoring server and rotate/revoke the key before expiration.

| Metric | Meaning and lifetime |
|---|---|
| `events_published_total`, `bytes_published_total` | Successful append/fsync operations and encoded record bytes, including API, replay, recovery appends and webhook DLQ writes; process lifetime. Deduplicated receipts and validation failures do not count. |
| `publish_latency_seconds` | Histogram of successful durable appends, including validation, lock wait, prepared metadata and fsync. No event payload scans. |
| `events_consumed_total` | Events returned by successful group pulls, including webhook pulls and redeliveries; process lifetime. This is not acknowledgements or unique application processing. |
| `dlq_events_total` | Dead-letter envelopes actually appended to `.DLQ` logs during this process; includes duplicate physical writes after uncertainty. |
| `broker_disk_bytes`, `partition_retained_events` | Actual retained log segment sizes/records, by workspace/topic/partition. Retention reduces these gauges. |
| `partition_oldest_offset`, `partition_next_offset` | Retained lower bound and exclusive high watermark. |
| `consumer_lag`, `consumer_committed_offset` | High watermark minus committed **next** offset, and committed next offset, by topic/group/partition. Lag includes offsets lost to retention. |
| `consumer_unavailable_events` | Unprocessed offset distance below the retained lower bound; requires explicit operator reset/recovery. No automatic skipping. |
| `consumer_inflight_partition`, `consumer_group_members`, `active_consumers` | Unexpired delivery leases/memberships. The same member ID in two groups counts twice. Scraping never rebalances or persists membership changes. |
| `active_topics`, `active_partitions`, `active_consumer_groups` | Current workspace metadata counts. |
| `webhook_delivery_total{outcome}`, `webhook_failure_total` | Durable recorded successful, failed or unknown attempt outcomes. Retried metadata writes cannot inflate them. Failure is recorded `retry` or `failed`; an unknown outcome is separate. |
| `webhook_dlq_routes_total` | Durable completed webhook DLQ transitions, distinct from physical DLQ log appends. |
| `broker_filesystem_available_bytes`, `broker_filesystem_capacity_bytes`, `broker_storage_pressure` | Node-level filesystem admission capacity; pressure means available bytes are below the configured write reserve. This is not a filesystem reservation against other processes. |
| `broker_uptime_seconds` | HTTP process uptime. |

Broker/pull counters restart at zero and are never reconstructed from offsets. Retained log bounds and committed offsets remain durable. Webhook counters live in a workspace rollup row updated in the same transaction as latest-state/history writes; scrapes do not scan the entire delivery history. The one-time migration seeds existing known transitions, including available legacy latest states. Earlier unavailable outcomes cannot be reconstructed. In-flight, delivered and recorded are distinct concepts: none of these metrics proves exactly-once receiver processing.

Metrics snapshot each subsystem under its own locks; a scrape is not one transaction spanning all producers and consumers. Latency bucket/count/sum snapshots are internally coherent. Metadata collection has a two-second timeout. Storage/coordinator/metadata errors produce HTTP503 before any successful metrics response is emitted. Prometheus should alert on `up == 0` as well as rising lag and storage pressure.

Useful PromQL: `rate(events_published_total[5m])`, `rate(bytes_published_total[5m])`, `sum(consumer_lag)`, `consumer_unavailable_events > 0`, and `histogram_quantile(0.95, sum by (le) (rate(publish_latency_seconds_bucket[5m])))`. Group label cardinality follows actual persisted groups; group lifecycle quotas remain a release hardening requirement.
