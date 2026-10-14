package chat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type paymentStore struct {
	fakeStore
	calls     int
	c         Conversation
	body, key string
}

func (p *paymentStore) ScheduleRequestPaymentLink(_ context.Context, c Conversation, body, key string, expiresAt time.Time) (Conversation, Message, bool, error) {
	p.calls++
	p.c = c
	p.body = body
	p.key = key
	return c, Message{ConversationID: c.ID, SenderKind: "system", Body: body, ClientMessageID: key}, true, nil
}
func TestPaymentLinkValidationAndScope(t *testing.T) {
	in := PaymentLinkInput{InvoiceExpiresAt: time.Now().Add(time.Hour), RequestID: uuid.New(), TenantID: uuid.New(), ParentUserID: uuid.New(), TransactionID: uuid.New(), CheckoutSessionURL: "https://pay.example/checkout?id=1"}
	source := SubjectContext{ID: in.RequestID, TenantID: in.TenantID, ParentID: in.ParentUserID, ClassName: "Private", StudentFirstName: "Kid"}
	cases := []struct {
		name   string
		mutate func(*PaymentLinkInput, *SubjectContext)
		want   error
	}{
		{"valid", func(*PaymentLinkInput, *SubjectContext) {}, nil},
		{"missing expiry", func(i *PaymentLinkInput, _ *SubjectContext) { i.InvoiceExpiresAt = time.Time{} }, ErrInvalid},
		{"expired", func(i *PaymentLinkInput, _ *SubjectContext) { i.InvoiceExpiresAt = time.Now().Add(-time.Second) }, ErrPaymentLinkInactive},
		{"wrong request", func(_ *PaymentLinkInput, s *SubjectContext) { s.ID = uuid.New() }, ErrNotFound},
		{"wrong tenant", func(_ *PaymentLinkInput, s *SubjectContext) { s.TenantID = uuid.New() }, ErrNotFound},
		{"wrong parent", func(_ *PaymentLinkInput, s *SubjectContext) { s.ParentID = uuid.New() }, ErrNotFound},
		{"missing transaction", func(i *PaymentLinkInput, _ *SubjectContext) { i.TransactionID = uuid.Nil }, ErrInvalid},
	}
	for _, raw := range []string{"", "javascript:alert(1)", "//pay.example/x", "https://user:pass@pay.example/x", "https:///x", "https://", "https://pay.example/\nfoo", "https://pay.example:bad/x", " https://pay.example/x", "https://pay.example/" + strings.Repeat("x", 1800)} {
		cases = append(cases, struct {
			name   string
			mutate func(*PaymentLinkInput, *SubjectContext)
			want   error
		}{"invalid URL " + raw, func(i *PaymentLinkInput, _ *SubjectContext) { i.CheckoutSessionURL = raw }, ErrInvalid})
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			input, subject := in, source
			tc.mutate(&input, &subject)
			store := &paymentStore{}
			svc := Service{Store: store, Academic: contextReader{item: subject}, Tenant: tenantReader{}}
			m, created, err := svc.SendScheduleRequestPaymentLink(context.Background(), input)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err=%v want=%v", err, tc.want)
			}
			if tc.want != nil {
				if store.calls != 0 {
					t.Fatal("wrote rejected input")
				}
				return
			}
			if !created || m.SenderKind != "system" || m.SenderUserID != nil || store.key != "private-payment:"+in.RequestID.String()+":"+in.TransactionID.String() || !strings.Contains(store.body, in.CheckoutSessionURL) {
				t.Fatalf("message=%+v key=%s", m, store.key)
			}
			var snapshot map[string]string
			if err := json.Unmarshal(store.c.Context, &snapshot); err != nil || snapshot["tenant_name"] != "Tenant A" || snapshot["class_name"] != "Private" {
				t.Fatalf("snapshot %s %v", store.c.Context, err)
			}
			if store.c.Kind != "schedule_request" || store.c.SubjectID != in.RequestID || store.c.TenantID != in.TenantID || *store.c.ParentUserID != in.ParentUserID {
				t.Fatal("bad conversation scope")
			}
		})
	}
}
func TestPaymentLinkDependenciesFailClosed(t *testing.T) {
	in := PaymentLinkInput{InvoiceExpiresAt: time.Now().Add(time.Hour), RequestID: uuid.New(), TenantID: uuid.New(), ParentUserID: uuid.New(), TransactionID: uuid.New(), CheckoutSessionURL: "http://localhost/checkout"}
	source := SubjectContext{ID: in.RequestID, TenantID: in.TenantID, ParentID: in.ParentUserID}
	for _, svc := range []Service{
		{Store: &fakeStore{}, Academic: contextReader{item: source}, Tenant: tenantReader{}},
		{Store: &paymentStore{}, Tenant: tenantReader{}},
		{Store: &paymentStore{}, Academic: contextReader{item: source}},
		{Store: &paymentStore{}, Academic: contextReader{err: ErrUnavailable}, Tenant: tenantReader{}},
	} {
		if _, _, err := svc.SendScheduleRequestPaymentLink(context.Background(), in); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("err=%v", err)
		}
	}
}
