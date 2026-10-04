# Security and operations

## Trust boundaries

API keys are random, 256-bit tokens stored as SHA-256 digests. Their scope and workspace are loaded from PostgreSQL on every authenticated request. Keys expire, revoke and track last-use time. Raw credentials are never stored in localStorage or request logs. Browser sessions are random HttpOnly SameSite=Strict cookies; mutations require the session CSRF token. Production cookies must be Secure and require HTTPS. Users use bcrypt cost 12. Roles are checked server-side through database memberships; submitted workspace fields cannot override the authenticated workspace.

Secrets for outbound webhooks are AES-256-GCM encrypted with a deployment-owned 32-byte key and domain-separated associated data. API responses exclude encrypted secrets. Keep the key outside source control and backed up securely. Signature verification must compare HMAC SHA-256 over `<unix-seconds>.<raw-body>` using constant-time comparison, reject timestamps more than five minutes old (and far-future timestamps), and deduplicate event IDs. Headers: `X-EventCore-Timestamp`, `X-EventCore-ID`, `X-EventCore-Signature: v1=<hex>`.

Webhook URLs require HTTPS/443, prohibit credentials, and do not follow redirects or environment proxies. The dialer resolves DNS at connection time, rejects any forbidden resolved address, and dials an approved IP directly while preserving TLS hostname verification. IPv4 private/link-local/loopback/shared/special ranges and IPv6 loopback/link-local/private/transition/metadata ranges are rejected. Tests inject an HTTP transport rather than disabling SSRF validation for local endpoints. Network egress controls remain advisable for deployments. Custom headers are not implemented yet, which also avoids credential-forwarding overrides in this version.

## Resource boundaries

Topic names are constrained and topic paths are constructed only server-side. HTTP bodies are limited to 2 MiB; batches to 100 events; ordinary topic event sizes to 1 MiB; schemas to 64 KiB; partition count to 256; topics to 100 per workspace. Pull batches are bounded, as are per-member deliveries; no whole topic is held in memory. SSE uses at most 64 connections globally and 16 per workspace and five-second write deadlines. Group membership is capped at 256. Login, IP, workspace and API-key requests have fixed-window limits with bounded limiter metadata. No external schema references can access files or network destinations.

The data directory requires an exclusive broker lock, private permissions and adequate available disk. Disk-pressure admission is not implemented yet: the current engine fences a partition on failed writes/fsync and readiness probes disk writes; operators must monitor capacity before exhaustion. Retention is segment-granular and cannot impose an exact active-file byte ceiling.

## Audit and diagnostics

Sensitive operations record `attempt.<action>` before mutation, so an audit-store failure prevents that mutation. These records describe attempted actions, including rejected or subsequently failed operations; they are not proof of successful completion. Audit success/failure outcomes and broader role/schema/DLQ management actions are pending. Database/host errors are logged internally; API responses avoid exposing connection strings and raw destination errors. Webhook errors show status/transport-failure category, not response bodies or secrets.

This is a development baseline requiring the hardening gaps in STATUS.md before public multi-tenant exposure. Report issues privately to the repository maintainers; do not include credentials or event payloads in public bug reports.
