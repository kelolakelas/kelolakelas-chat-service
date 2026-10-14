package chat

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
)

var ErrPaymentLinkInactive = errors.New("Payment link is inactive")

// PaymentLinkStore atomically checks frozen participants and expiry at the write
// boundary (after lock waits, including retries), then inserts idempotently.
// last_message_at changes only on insert.
type PaymentLinkStore interface {
	ScheduleRequestPaymentLink(context.Context, Conversation, string, string, time.Time) (Conversation, Message, bool, error)
}

type PaymentLinkInput struct {
	InvoiceExpiresAt   time.Time `json:"invoice_expires_at"`
	RequestID          uuid.UUID `json:"request_id"`
	TenantID           uuid.UUID `json:"tenant_id"`
	ParentUserID       uuid.UUID `json:"parent_user_id"`
	TransactionID      uuid.UUID `json:"transaction_id"`
	CheckoutSessionURL string    `json:"checkout_session_url"`
}

// SendScheduleRequestPaymentLink accepts only trusted service input; the academic
// context is independently checked before any write. Billing eligibility is the
// calling academic service's responsibility.
func (s Service) SendScheduleRequestPaymentLink(ctx context.Context, in PaymentLinkInput) (Message, bool, error) {
	if in.RequestID == uuid.Nil || in.TenantID == uuid.Nil || in.ParentUserID == uuid.Nil || in.TransactionID == uuid.Nil {
		return Message{}, false, ErrInvalid
	}
	if in.InvoiceExpiresAt.IsZero() {
		return Message{}, false, ErrInvalid
	}
	if !in.InvoiceExpiresAt.After(time.Now()) {
		return Message{}, false, ErrPaymentLinkInactive
	}
	u, err := url.Parse(in.CheckoutSessionURL)
	if err != nil || len(in.CheckoutSessionURL) > 1800 || strings.TrimSpace(in.CheckoutSessionURL) != in.CheckoutSessionURL || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Opaque != "" {
		return Message{}, false, ErrInvalid
	}
	store, ok := s.Store.(PaymentLinkStore)
	if !ok || s.Academic == nil || s.Tenant == nil {
		return Message{}, false, ErrUnavailable
	}
	source, err := s.Academic.Context(ctx, "schedule_request", in.RequestID)
	if err != nil {
		return Message{}, false, err
	}
	if source.ID != in.RequestID || source.TenantID != in.TenantID || source.ParentID != in.ParentUserID {
		return Message{}, false, ErrNotFound
	}
	name, err := s.Tenant.Name(ctx, in.TenantID)
	if err != nil {
		return Message{}, false, ErrUnavailable
	}
	snapshot, err := json.Marshal(map[string]string{"class_name": source.ClassName, "student_first_name": source.StudentFirstName, "report_title": source.Title, "tenant_name": name})
	if err != nil {
		return Message{}, false, ErrUnavailable
	}
	c := Conversation{TenantID: in.TenantID, Kind: "schedule_request", SubjectID: in.RequestID, ParentUserID: &in.ParentUserID, CreatedByUserID: in.ParentUserID, Context: snapshot}
	key := "private-payment:" + in.RequestID.String() + ":" + in.TransactionID.String()
	c, m, created, err := store.ScheduleRequestPaymentLink(ctx, c, "Link pembayaran kelas privat: "+in.CheckoutSessionURL, key, in.InvoiceExpiresAt)
	if err != nil {
		return Message{}, false, err
	}
	if created && s.Hub != nil {
		s.Hub.BroadcastMessage(c, m)
	}
	return m, created, nil
}
