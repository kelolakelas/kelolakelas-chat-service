package chathttp

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type paymentHTTPStore struct {
	stubStore
	calls int
}

func (s *paymentHTTPStore) ScheduleRequestPaymentLink(_ context.Context, c chat.Conversation, body, key string, expiresAt time.Time) (chat.Conversation, chat.Message, bool, error) {
	s.calls++
	return c, chat.Message{ID: uuid.New(), SenderKind: "system", Body: body, ClientMessageID: key}, s.calls == 1, nil
}

type paymentAcademic struct{ source chat.SubjectContext }

func (a paymentAcademic) Context(_ context.Context, kind string, id uuid.UUID) (chat.SubjectContext, error) {
	if kind != "schedule_request" || id != a.source.ID {
		return chat.SubjectContext{}, chat.ErrNotFound
	}
	return a.source, nil
}

type paymentTenant struct{}

func (paymentTenant) Name(context.Context, uuid.UUID) (string, error) { return "School", nil }
func TestInternalPaymentLinkHTTP(t *testing.T) {
	in := chat.PaymentLinkInput{InvoiceExpiresAt: time.Now().Add(time.Hour), RequestID: uuid.New(), TenantID: uuid.New(), ParentUserID: uuid.New(), TransactionID: uuid.New(), CheckoutSessionURL: "https://pay.example/x"}
	raw, _ := json.Marshal(in)
	withoutExpiry := in
	withoutExpiry.InvoiceExpiresAt = time.Time{}
	missingRaw, _ := json.Marshal(withoutExpiry)
	expired := in
	expired.InvoiceExpiresAt = time.Now().Add(-time.Second)
	expiredRaw, _ := json.Marshal(expired)
	for _, tc := range []struct {
		name, credential, configured, method, body string
		want                                       int
	}{
		{"success", "shared", "shared", "POST", string(raw), 201},
		{"missing expiry", "shared", "shared", "POST", string(missingRaw), 400},
		{"expired", "shared", "shared", "POST", string(expiredRaw), 409},
		{"malformed expiry", "shared", "shared", "POST", strings.Replace(string(raw), in.InvoiceExpiresAt.Format(time.RFC3339Nano), "invalid", 1), 400},
		{"null expiry", "shared", "shared", "POST", strings.Replace(string(raw), `"`+in.InvoiceExpiresAt.Format(time.RFC3339Nano)+`"`, "null", 1), 400},
		{"missing credential", "", "shared", "POST", string(raw), 401},
		{"bad credential", "wrong", "shared", "POST", string(raw), 401},
		{"disabled", "shared", "", "POST", string(raw), 401},
		{"wrong method", "shared", "shared", "GET", string(raw), 405},
		{"caller key rejected", "shared", "shared", "POST", strings.TrimSuffix(string(raw), "}") + `,"idempotency_key":"evil"}`, 400},
		{"malformed", "shared", "shared", "POST", `{"request_id":"bad"}`, 400},
		{"trailing JSON", "shared", "shared", "POST", string(raw) + `{}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &paymentHTTPStore{}
			h := Handler{InternalCredential: tc.configured, Service: chat.Service{Store: store, Academic: paymentAcademic{chat.SubjectContext{ID: in.RequestID, TenantID: in.TenantID, ParentID: in.ParentUserID}}, Tenant: paymentTenant{}}}
			req := httptest.NewRequest(tc.method, "/internal/schedule-request-payment-links", strings.NewReader(tc.body))
			req.Header.Set("X-Internal-Service-Credential", tc.credential)
			req.Header.Set("Authorization", "Bearer user-token-is-not-an-internal-credential")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.want != 201 && store.calls != 0 {
				t.Fatal("rejected request wrote message")
			}
			if tc.want == 201 {
				req = httptest.NewRequest("POST", "/internal/schedule-request-payment-links", strings.NewReader(string(raw)))
				req.Header.Set("X-Internal-Service-Credential", "shared")
				w = httptest.NewRecorder()
				h.ServeHTTP(w, req)
				if w.Code != 200 {
					t.Fatalf("retry status=%d", w.Code)
				}
			}
		})
	}
}
