# kelolakelas-chat-service

Service chat realtime KelolaKelas antara tenant, pengajar, dan parent (Linear project
"Chat realtime antara tenant, pengajar, dan parent").

Status: kerangka awal. Saat ini hanya ada `GET /health` dan graceful shutdown. Domain
percakapan, pesan, REST API, dan WebSocket dibangun oleh issue di project tersebut.

## Menjalankan

```bash
cp .env.example .env   # opsional; PORT default 8083
make run
make test
```

## Internal private-payment messages

`POST /internal/schedule-request-payment-links` is a service-to-service endpoint,
not a public gateway route. It requires `X-Internal-Service-Credential` matching
`INTERNAL_SERVICE_CREDENTIAL`; an empty configuration fails closed. User JWTs do
not authorize it.

Body fields: `request_id`, `tenant_id`, `parent_user_id`, `transaction_id` (all
non-zero UUIDs), and `checkout_session_url` (HTTP/HTTPS URL without credentials,
maximum 1800 bytes), plus required `invoice_expires_at` (non-null RFC3339 timestamp
from billing). Unknown fields are rejected. Expiry is checked with PostgreSQL
`clock_timestamp()` after conversation lock acquisition and at message insertion;
expired sends and retries return HTTP 409 without persisting changes. The academic caller must first
validate approval and the current billing invoice; chat independently checks
academic schedule-request context ownership against the supplied tenant/parent.

The endpoint atomically creates the request conversation if absent, using the
academic context and tenant name snapshot, and inserts a `system` message with
no sender user. Existing frozen participants must match. The server derives
`private-payment:<request_id>:<transaction_id>` as the idempotency key: retries
return the original message without changing its URL, timestamp or broadcasting
again. First insertion returns HTTP 201; retries return 200. Existing
schedule-request parent/tenant visibility is unchanged. No migration is needed;
the existing system-message partial unique index provides deduplication.

PostgreSQL tests (including concurrent initial sends) run when
`TEST_DATABASE_URL` points to a disposable database with all migrations applied.
Existing integration tests truncate tables; do not use a shared database.

CI job `gate` (`.github/workflows/ci.yml`) sama dengan service Go lain: `go mod verify`,
`gofmt`, `go vet`, `go test -race`, `go build`, dan `govulncheck`.
