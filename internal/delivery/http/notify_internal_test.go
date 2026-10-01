package chathttp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

// internalStubStore records Notify calls from the internal endpoint.
type internalStubStore struct {
	stubStore
	notifyErr            error
	gotTenant, gotParent uuid.UUID
	gotBody, gotKey      string
	calls                int
}

func (s *internalStubStore) Notify(_ context.Context, tenant, parent uuid.UUID, body, key string) (chat.Conversation, chat.Message, bool, error) {
	s.calls++
	s.gotTenant, s.gotParent, s.gotBody, s.gotKey = tenant, parent, body, key
	if s.notifyErr != nil {
		return chat.Conversation{}, chat.Message{}, false, s.notifyErr
	}
	m := chat.Message{ID: uuid.New(), ConversationID: uuid.New(), SenderUserID: nil, SenderKind: "system", Body: body, ClientMessageID: key}
	return chat.Conversation{ID: m.ConversationID}, m, true, nil
}

func internalHandler(store chat.Store) Handler {
	return Handler{Secret: "secret", InternalCredential: "shared-secret", Service: chat.Service{Store: store}}
}

func validNotifyBody() string {
	return `{"tenant_id":"` + uuid.New().String() + `","parent_user_id":"` + uuid.New().String() + `","body":"Jadwal berubah","idempotency_key":"k1"}`
}

func TestInternalNotifyAuth(t *testing.T) {
	cases := []struct {
		name, credential, body string
		want                   int
	}{
		{"missing credential", "", validNotifyBody(), 401},
		{"wrong credential", "not-the-secret", validNotifyBody(), 401},
		{"empty header value", "", validNotifyBody(), 401},
		{"valid credential", "shared-secret", validNotifyBody(), 201},
		{"unknown field", "shared-secret", `{"tenant_id":"` + uuid.New().String() + `","parent_user_id":"` + uuid.New().String() + `","body":"hi","idempotency_key":"k1","extra":1}`, 400},
		{"bad tenant id", "shared-secret", `{"tenant_id":"nope","parent_user_id":"` + uuid.New().String() + `","body":"hi","idempotency_key":"k1"}`, 400},
		{"nil tenant id", "shared-secret", `{"tenant_id":"00000000-0000-0000-0000-000000000000","parent_user_id":"` + uuid.New().String() + `","body":"hi","idempotency_key":"k1"}`, 400},
		{"bad parent id", "shared-secret", `{"tenant_id":"` + uuid.New().String() + `","parent_user_id":"","body":"hi","idempotency_key":"k1"}`, 400},
		{"missing key", "shared-secret", `{"tenant_id":"` + uuid.New().String() + `","parent_user_id":"` + uuid.New().String() + `","body":"hi"}`, 400},
		{"empty body", "shared-secret", `{"tenant_id":"` + uuid.New().String() + `","parent_user_id":"` + uuid.New().String() + `","body":"  ","idempotency_key":"k1"}`, 400},
		{"trailing JSON", "shared-secret", validNotifyBody() + `{"again":1}`, 400},
		{"malformed JSON", "shared-secret", `{`, 400},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &internalStubStore{}
			h := internalHandler(store)
			req := httptest.NewRequest("POST", "/internal/notifications", bytes.NewBufferString(tc.body))
			if tc.credential != "" {
				req.Header.Set("X-Internal-Service-Credential", tc.credential)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("code=%d want %d body=%s", rec.Code, tc.want, rec.Body.String())
			}
			if tc.want == 401 && store.calls != 0 {
				t.Fatalf("store called %d times without valid credential", store.calls)
			}
			if strings.Contains(rec.Body.String(), "shared-secret") || strings.Contains(rec.Body.String(), "not-the-secret") {
				t.Fatal("credential material leaked in response")
			}
		})
	}
}

func TestInternalNotifyCreatesSystemMessage(t *testing.T) {
	store := &internalStubStore{}
	h := internalHandler(store)
	tenant, parent := uuid.New(), uuid.New()
	body := `{"tenant_id":"` + tenant.String() + `","parent_user_id":"` + parent.String() + `","body":"Jadwal les besaar pindah","idempotency_key":"sched-42"}`
	req := httptest.NewRequest("POST", "/internal/notifications", bytes.NewBufferString(body))
	req.Header.Set("X-Internal-Service-Credential", "shared-secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 201 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
	var envelope struct {
		Status string       `json:"status"`
		Data   chat.Message `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if store.calls != 1 || store.gotTenant != tenant || store.gotParent != parent || store.gotKey != "sched-42" {
		t.Fatalf("captured: %+v", store)
	}
	if envelope.Data.SenderUserID != nil || envelope.Data.SenderKind != "system" {
		t.Fatalf("message shape: %+v", envelope.Data)
	}
	if envelope.Status != "success" {
		t.Fatalf("status: %s", envelope.Status)
	}
}

func TestInternalNotifyUnconfiguredCredential(t *testing.T) {
	h := Handler{Secret: "secret", Service: chat.Service{Store: &internalStubStore{}}}
	req := httptest.NewRequest("POST", "/internal/notifications", bytes.NewBufferString(validNotifyBody()))
	req.Header.Set("X-Internal-Service-Credential", "anything")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 401 {
		t.Fatalf("unconfigured credential must fail closed: code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInternalNotifyMethodNotAllowed(t *testing.T) {
	h := internalHandler(&internalStubStore{})
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		req := httptest.NewRequest(method, "/internal/notifications", nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 405 {
			t.Fatalf("%s: code=%d body=%s", method, rec.Code, rec.Body.String())
		}
	}
}

func TestNotificationKindNotUserCreatable(t *testing.T) {
	// The public create allowlist must never accept kind=notification: those
	// conversations are born only from the internal notify path.
	h := Handler{Secret: "secret", Service: chat.Service{Store: stubStore{}, Permission: down{}}}
	for _, kind := range []string{"notification", "Notification", "system"} {
		req := httptest.NewRequest("POST", "/api/v1/chat/conversations", bytes.NewBufferString(`{"kind":"`+kind+`","subject_id":"`+uuid.NewString()+`"}`))
		req.Header.Set("Authorization", signed(uuid.NewString(), uuid.NewString(), "", "", false))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != 400 {
			t.Fatalf("kind %q: code=%d body=%s", kind, rec.Code, rec.Body.String())
		}
	}
}

func TestParentReplyToNotificationForbidden(t *testing.T) {
	tenant, parent := uuid.New(), uuid.New()
	id := uuid.New()
	conv := chat.Conversation{ID: id, TenantID: tenant, Kind: "notification", SubjectID: parent, ParentUserID: &parent}
	h := Handler{Secret: "secret", Service: chat.Service{Store: stubStore{c: conv}, Permission: down{}}}
	req := httptest.NewRequest("POST", "/api/v1/chat/conversations/"+id.String()+"/messages", bytes.NewBufferString(`{"body":"balasan","client_message_id":"c1"}`))
	req.Header.Set("Authorization", signed(parent.String(), "", "", "", true))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 403 {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestInternalNotifyServiceErrorMapping(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want int
	}{
		{"invalid", chat.ErrInvalid, 400},
		{"unavailable", chat.ErrUnavailable, 503},
		{"other", errors.New("boom"), 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := internalHandler(&internalStubStore{notifyErr: tc.err})
			req := httptest.NewRequest("POST", "/internal/notifications", bytes.NewBufferString(validNotifyBody()))
			req.Header.Set("X-Internal-Service-Credential", "shared-secret")
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("code=%d want %d body=%s", rec.Code, tc.want, rec.Body.String())
			}
			if tc.name == "other" && strings.Contains(rec.Body.String(), "boom") {
				t.Fatal("internal error leaked")
			}
		})
	}
}
