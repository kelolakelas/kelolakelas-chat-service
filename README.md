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

CI job `gate` (`.github/workflows/ci.yml`) sama dengan service Go lain: `go mod verify`,
`gofmt`, `go vet`, `go test -race`, `go build`, dan `govulncheck`.
