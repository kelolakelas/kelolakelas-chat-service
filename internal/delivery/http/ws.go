package chathttp

import (
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

// WS connection parameters from the KEL-121 contract.
const (
	// defaultWSPingInterval is the server ping cadence.
	defaultWSPingInterval = 30 * time.Second
	// defaultWSConnLifetime caps a connection at 30 minutes; the client then
	// mints a fresh ticket and reconnects.
	defaultWSConnLifetime = 30 * time.Minute
	// defaultWSClientFrameLimit caps inbound client frames at 4 KB. The
	// socket is server-to-client only, so any larger frame is abuse.
	defaultWSClientFrameLimit = 4 << 10
)

// wsParams are the per-connection liveness parameters. Production uses the
// defaults; tests shorten them on their own handler instance instead of
// mutating package state (hijacked connections outlive httptest.Server.Close
// and would race a global write).
type wsParams struct {
	pingInterval     time.Duration
	connLifetime     time.Duration
	clientFrameLimit int64
}

func defaultWSParams() wsParams {
	return wsParams{pingInterval: defaultWSPingInterval, connLifetime: defaultWSConnLifetime, clientFrameLimit: defaultWSClientFrameLimit}
}

// wsPongWait is the deadline for the next pong; two missed intervals
// (one grace period) close the connection.
func (p wsParams) pongWait() time.Duration { return 2 * p.pingInterval }

// wsUpgrader allows cross-origin upgrades; authentication is the single-use
// ticket, not cookies, so the Origin header carries no authority.
//
// WS library choice (KEL-121): gorilla/websocket, the long-standing de-facto
// Go WS standard with a stable API and no known govulncheck findings at
// v1.5.3. Only its server upgrade, ping/pong, read-limit, and close-code
// primitives are used; client-initiated messaging stays on REST by design.
var wsUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// issueWSTicket handles POST /api/v1/chat/ws-tickets behind JWT. It binds the
// ticket to the full actor (user, tenant, role, member, is_parent) plus the
// token expiry, and returns the plaintext ticket once with its expiry time.
func (h Handler) issueWSTicket(w http.ResponseWriter, r *http.Request) {
	if h.Tickets == nil {
		respond(w, 500, "Internal server error", nil)
		return
	}
	a, exp, ok := h.parseAuth(r)
	if !ok {
		respond(w, 401, "Invalid or expired token", nil)
		return
	}
	ticket, expiresAt := h.Tickets.Issue(a, exp)
	if ticket == "" {
		respond(w, 500, "Internal server error", nil)
		return
	}
	respond(w, 200, "OK", map[string]any{"ticket": ticket, "expires_at": expiresAt.UTC().Format(time.RFC3339Nano)})
}

// serveWS handles GET /api/v1/chat/ws?ticket=. It validates and atomically
// consumes the ticket BEFORE any upgrade; failures answer 401 with no
// upgrade. On success it freezes the connect-time rights, registers with the
// hub (5-conn cap per user), and pumps server-to-client events until token
// expiry, the 30-minute lifetime, a missed pong, or shutdown.
func (h Handler) serveWS(w http.ResponseWriter, r *http.Request) {
	if h.Tickets == nil || h.Hub == nil {
		respond(w, 500, "Internal server error", nil)
		return
	}
	ticket := r.URL.Query().Get("ticket")
	if ticket == "" {
		respond(w, 401, "Invalid or expired ticket", nil)
		return
	}
	actor, tokenExp, ok := h.Tickets.Consume(ticket)
	if !ok {
		respond(w, 401, "Invalid or expired ticket", nil)
		return
	}
	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()
	// Freeze chat:manage/report:read rights at connect time, per the
	// contract; the hub fan-out consults this copy for every event.
	actor = h.Service.Rights(r.Context(), actor)
	sub, ok := h.Hub.Register(actor)
	if !ok {
		_ = conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "connection limit"),
			time.Now().Add(5*time.Second))
		return
	}
	defer h.Hub.Unregister(sub)
	params := defaultWSParams()
	if h.wsParams != nil {
		params = *h.wsParams
	}
	pumpWSConn(conn, sub, tokenExp, params)
}

// pumpWSConn relays hub events to one connection and enforces liveness:
// 30s pings, close after two missed pong intervals, close at token expiry,
// and close at the 30-minute lifetime. Inbound frames are drained (and
// discarded) up to 4 KB so a client cannot stall the socket; any content
// besides pong is ignored because the socket is server-to-client only.
func pumpWSConn(conn *websocket.Conn, sub *chat.WSConnection, tokenExp time.Time, params wsParams) {
	deadline := time.Now().Add(params.connLifetime)
	if !tokenExp.IsZero() && tokenExp.Before(deadline) {
		deadline = tokenExp
	}
	conn.SetReadLimit(params.clientFrameLimit)
	_ = conn.SetReadDeadline(time.Now().Add(params.pongWait()))
	conn.SetPongHandler(func(string) error {
		_ = conn.SetReadDeadline(time.Now().Add(params.pongWait()))
		return nil
	})
	// done fires when the read loop ends (client went away, oversize frame,
	// or a missed pong tripped the read deadline). The pump selects on it so
	// a dead connection is reaped promptly instead of lingering until the
	// next ping write fails.
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer conn.Close()
		for {
			if _, _, err := conn.NextReader(); err != nil {
				return
			}
		}
	}()
	ticker := time.NewTicker(params.pingInterval)
	defer ticker.Stop()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for {
		select {
		case <-done:
			return
		case payload, ok := <-sub.Out():
			if !ok {
				_ = conn.WriteControl(websocket.CloseMessage,
					websocket.FormatCloseMessage(websocket.CloseNormalClosure, "shutdown"),
					time.Now().Add(5*time.Second))
				return
			}
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-timer.C:
			_ = conn.WriteControl(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, "lifetime"),
				time.Now().Add(5*time.Second))
			return
		}
	}
}
