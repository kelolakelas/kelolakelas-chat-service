package chathttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

// wsPerm answers identity permission checks from a static matrix.
type wsPerm map[string]bool

func (p wsPerm) CheckPermission(_ context.Context, _, _, _, name string) (bool, error) {
	return p[name], nil
}

type wsStore struct{ c chat.Conversation }

func (s wsStore) Create(_ context.Context, c chat.Conversation) (chat.Conversation, bool, error) {
	c.ID = uuid.New()
	return c, true, nil
}
func (s wsStore) Get(_ context.Context, _ uuid.UUID) (chat.Conversation, error) { return s.c, nil }
func (s wsStore) List(_ context.Context, _ chat.Actor, _, _ int) ([]chat.Conversation, error) {
	return []chat.Conversation{s.c}, nil
}
func (s wsStore) ListOwner(_ context.Context, _ chat.Actor, _, _ int) ([]chat.Conversation, error) {
	return []chat.Conversation{s.c}, nil
}
func (s wsStore) Send(_ context.Context, id uuid.UUID, a chat.Actor, body, key string) (chat.Message, error) {
	return chat.Message{ID: uuid.New(), ConversationID: id, SenderUserID: &a.UserID, SenderKind: "member", Body: body, ClientMessageID: key, CreatedAt: time.Now()}, nil
}
func (s wsStore) Messages(_ context.Context, _ uuid.UUID, _ *uuid.UUID, _ int) ([]chat.Message, error) {
	return nil, nil
}
func (s wsStore) Read(_ context.Context, _, _ uuid.UUID) error { return nil }
func (s wsStore) Notify(_ context.Context, _, _ uuid.UUID, _, _ string) (chat.Conversation, chat.Message, bool, error) {
	return chat.Conversation{}, chat.Message{}, false, chat.ErrInvalid
}

func signExp(user, tenant, role, member string, parent bool, exp time.Time) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{UserID: user, TenantID: tenant, RoleID: role, MemberID: member, IsParent: parent, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(exp)}})
	value, err := token.SignedString([]byte("secret"))
	if err != nil {
		panic(err)
	}
	return "Bearer " + value
}

func wsFixture(conv chat.Conversation, perms wsPerm) Handler {
	h := Handler{Secret: "secret", Service: chat.Service{Store: wsStore{c: conv}, Permission: perms}, Tickets: chat.NewTicketStore(), Hub: chat.NewWSHub()}
	h.Service.Hub = h.Hub
	return h
}

func issueWSTicket(t *testing.T, h Handler, auth string) string {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/chat/ws-tickets", nil)
	req.Header.Set("Authorization", auth)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("issue ticket: code=%d body=%s", rec.Code, rec.Body.String())
	}
	var envelope struct {
		Data struct {
			Ticket    string `json:"ticket"`
			ExpiresAt string `json:"expires_at"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode ticket: %v", err)
	}
	if envelope.Data.Ticket == "" {
		t.Fatalf("empty ticket: %s", rec.Body.String())
	}
	exp, err := time.Parse(time.RFC3339Nano, envelope.Data.ExpiresAt)
	if err != nil {
		t.Fatalf("bad expires_at %q: %v", envelope.Data.ExpiresAt, err)
	}
	if ttl := time.Until(exp); ttl <= 0 || ttl > 60*time.Second {
		t.Fatalf("ticket TTL out of range: %v", ttl)
	}
	return envelope.Data.Ticket
}

func dialWS(t *testing.T, srv *httptest.Server, ticket string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/v1/chat/ws?ticket=" + ticket
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		if resp != nil {
			t.Fatalf("dial: %v (status=%d)", err, resp.StatusCode)
		}
		t.Fatalf("dial: %v", err)
	}
	return conn
}

func readWSEvent(t *testing.T, conn *websocket.Conn, timeout time.Duration) chat.WSEvent {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	var ev chat.WSEvent
	if err := conn.ReadJSON(&ev); err != nil {
		t.Fatalf("read event: %v", err)
	}
	return ev
}

func expectNoEvent(t *testing.T, conn *websocket.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
	var ev chat.WSEvent
	if err := conn.ReadJSON(&ev); err == nil {
		t.Fatalf("leaked event to unauthorized connection: %+v", ev)
	}
}

func restSend(t *testing.T, srv *httptest.Server, convID uuid.UUID, auth, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/chat/conversations/"+convID.String()+"/messages",
		strings.NewReader(`{"body":`+strconv_quote(body)+`,"client_message_id":"k-1"}`))
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rest send: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("rest send: status=%d", resp.StatusCode)
	}
}

func strconv_quote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func TestWSMessageCreatedFanoutAndTenantIsolation(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	convID := uuid.New()
	conv := chat.Conversation{ID: convID, TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	managerID := uuid.New()
	role, member := uuid.NewString(), uuid.NewString()
	perms := wsPerm{"chat:manage": true}
	h := wsFixture(conv, perms)
	srv := httptest.NewServer(h)
	defer srv.Close()

	ownerAuth := signed(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	managerAuth := signed(managerID.String(), tenant.String(), role, member, false)
	outsiderAuth := signed(uuid.NewString(), uuid.NewString(), uuid.NewString(), uuid.NewString(), false)

	ownerConn := dialWS(t, srv, issueWSTicket(t, h, ownerAuth))
	defer ownerConn.Close()
	managerConn := dialWS(t, srv, issueWSTicket(t, h, managerAuth))
	defer managerConn.Close()
	outsiderConn := dialWS(t, srv, issueWSTicket(t, h, outsiderAuth))
	defer outsiderConn.Close()

	// Another participant sends via REST; both visible connections get
	// message.created without reload, the other-tenant connection gets nothing.
	restSend(t, srv, convID, managerAuth, "halo realtime")

	ev := readWSEvent(t, ownerConn, 3*time.Second)
	if ev.Type != chat.WSEventMessageCreated || ev.ConversationID != convID || ev.Message == nil || ev.Message.Body != "halo realtime" {
		t.Fatalf("owner event: %+v", ev)
	}
	ev = readWSEvent(t, managerConn, 3*time.Second)
	if ev.Type != chat.WSEventMessageCreated || ev.ConversationID != convID {
		t.Fatalf("manager event: %+v", ev)
	}
	expectNoEvent(t, outsiderConn)
}

func TestWSConversationReadFanoutAndParentIsolation(t *testing.T) {
	tenant := uuid.New()
	parent := uuid.New()
	otherParent := uuid.New()
	convID := uuid.New()
	conv := chat.Conversation{ID: convID, TenantID: tenant, Kind: "schedule_request", ParentUserID: &parent}
	staffID := uuid.New()
	staffAuth := signed(staffID.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	h := wsFixture(conv, wsPerm{"chat:manage": true})
	srv := httptest.NewServer(h)
	defer srv.Close()

	ownerConn := dialWS(t, srv, issueWSTicket(t, h, signed(parent.String(), "", "", "", true)))
	defer ownerConn.Close()
	staffConn := dialWS(t, srv, issueWSTicket(t, h, staffAuth))
	defer staffConn.Close()
	otherConn := dialWS(t, srv, issueWSTicket(t, h, signed(otherParent.String(), "", "", "", true)))
	defer otherConn.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/api/v1/chat/conversations/"+convID.String()+"/read", nil)
	req.Header.Set("Authorization", staffAuth)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("rest read: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rest read: status=%d", resp.StatusCode)
	}

	ev := readWSEvent(t, ownerConn, 3*time.Second)
	if ev.Type != chat.WSEventConversationRead || ev.ConversationID != convID || ev.UserID == nil || *ev.UserID != staffID || ev.ReadAt == nil {
		t.Fatalf("owner read event: %+v", ev)
	}
	if ev := readWSEvent(t, staffConn, 3*time.Second); ev.Type != chat.WSEventConversationRead {
		t.Fatalf("staff read event: %+v", ev)
	}
	expectNoEvent(t, otherConn)
}

func TestWSTicketRejected(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := chat.Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	h := wsFixture(conv, wsPerm{})
	auth := signed(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)

	// Ticket endpoint itself requires JWT.
	req := httptest.NewRequest(http.MethodPost, "/api/v1/chat/ws-tickets", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("ticket without JWT: code=%d", rec.Code)
	}
	// Wrong method on both WS routes.
	for _, tc := range []struct{ method, path string }{{"GET", "/api/v1/chat/ws-tickets"}, {"POST", "/api/v1/chat/ws"}} {
		req := httptest.NewRequest(tc.method, tc.path, nil)
		req.Header.Set("Authorization", auth)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s %s: code=%d", tc.method, tc.path, rec.Code)
		}
	}

	getWS := func(ticket string) int {
		target := "/api/v1/chat/ws"
		if ticket != "" {
			target += "?ticket=" + ticket
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
		return rec.Code
	}
	if code := getWS(""); code != http.StatusUnauthorized {
		t.Fatalf("missing ticket: code=%d", code)
	}
	if code := getWS("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"); code != http.StatusUnauthorized {
		t.Fatalf("random ticket: code=%d", code)
	}
	// A ticket minted from an already-expired token burns on first use.
	expiredAuth := signExp(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false, time.Now().Add(-time.Second))
	rec2 := httptest.NewRecorder()
	expiredReq := httptest.NewRequest(http.MethodPost, "/api/v1/chat/ws-tickets", nil)
	expiredReq.Header.Set("Authorization", expiredAuth)
	h.ServeHTTP(rec2, expiredReq)
	if rec2.Code != http.StatusUnauthorized {
		t.Fatalf("ticket issue with expired JWT: code=%d", rec2.Code)
	}

	// Single use: first dial consumes, second attempt with the same ticket
	// is rejected 401 with no upgrade.
	srv := httptest.NewServer(h)
	defer srv.Close()
	ticket := issueWSTicket(t, h, auth)
	conn := dialWS(t, srv, ticket)
	defer conn.Close()
	if code := getWS(ticket); code != http.StatusUnauthorized {
		t.Fatalf("reused ticket: code=%d", code)
	}
}

func TestWSConnLimitPerUser(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := chat.Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	h := wsFixture(conv, wsPerm{})
	auth := signed(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	srv := httptest.NewServer(h)
	defer srv.Close()

	var conns []*websocket.Conn
	for i := 0; i < chat.WSMaxConnsPerUser; i++ {
		conns = append(conns, dialWS(t, srv, issueWSTicket(t, h, auth)))
	}
	defer func() {
		for _, c := range conns {
			c.Close()
		}
	}()
	// The 6th upgrade succeeds at HTTP level but is refused with a policy
	// violation close frame.
	extra := dialWS(t, srv, issueWSTicket(t, h, auth))
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, _, err := extra.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok || closeErr.Code != websocket.ClosePolicyViolation {
		t.Fatalf("6th conn: err=%v", err)
	}
}

func TestWSCloseOnTokenExpiry(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := chat.Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	h := wsFixture(conv, wsPerm{})
	auth := signExp(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false, time.Now().Add(time.Second))
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn := dialWS(t, srv, issueWSTicket(t, h, auth))
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok || closeErr.Code != websocket.CloseNormalClosure {
		t.Fatalf("token expiry close: err=%v", err)
	}
}

func TestWSCloseOnLifetime(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := chat.Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	h := wsFixture(conv, wsPerm{})
	short := defaultWSParams()
	short.connLifetime = 400 * time.Millisecond
	h.wsParams = &short
	auth := signed(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn := dialWS(t, srv, issueWSTicket(t, h, auth))
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	_, _, err := conn.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok || closeErr.Code != websocket.CloseNormalClosure {
		t.Fatalf("lifetime close: err=%v", err)
	}
}

func TestWSPingKeepsConnAlive(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	convID := uuid.New()
	conv := chat.Conversation{ID: convID, TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	h := wsFixture(conv, wsPerm{})
	// Shorten only the ping cadence (on this handler instance, not package
	// state); the pong deadline stays two intervals so a live client survives.
	fast := defaultWSParams()
	fast.pingInterval = 100 * time.Millisecond
	h.wsParams = &fast
	auth := signed(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn := dialWS(t, srv, issueWSTicket(t, h, auth))
	defer conn.Close()
	// Hold one blocking read the way a real client does: gorilla answers
	// server pings with pongs only while a read is in flight, and the pong
	// deadline is two intervals. A missed pong would have reaped the
	// connection well before the REST send below.
	got := make(chan chat.WSEvent, 1)
	readErr := make(chan error, 1)
	go func() {
		var ev chat.WSEvent
		if err := conn.ReadJSON(&ev); err != nil {
			readErr <- err
			return
		}
		got <- ev
	}()
	time.Sleep(600 * time.Millisecond)
	restSend(t, srv, convID, auth, "masih hidup")
	select {
	case ev := <-got:
		if ev.Type != chat.WSEventMessageCreated {
			t.Fatalf("event after pings: %+v", ev)
		}
	case err := <-readErr:
		t.Fatalf("conn reaped despite live pongs: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("no event after keepalive window")
	}
}

func TestWSOversizeFrameCloses(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	conv := chat.Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &owner}
	h := wsFixture(conv, wsPerm{})
	auth := signed(owner.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	srv := httptest.NewServer(h)
	defer srv.Close()

	conn := dialWS(t, srv, issueWSTicket(t, h, auth))
	defer conn.Close()
	// The socket is server-to-client only; a frame past the 4 KB cap trips
	// the read limit and the server reaps the connection.
	if err := conn.WriteMessage(websocket.BinaryMessage, make([]byte, 8<<10)); err != nil {
		t.Fatalf("write oversize: %v", err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	if _, _, err := conn.ReadMessage(); err == nil {
		t.Fatal("oversize frame did not close the connection")
	}
}
