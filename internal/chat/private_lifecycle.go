package chat

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"strings"
	"time"
	"unicode/utf8"
)

type PrivateLifecycleInput struct {
	EventID   uuid.UUID `json:"event_id"`
	RequestID uuid.UUID `json:"request_id"`
	TenantID  uuid.UUID `json:"tenant_id"`
	ParentID  uuid.UUID `json:"parent_id"`
}

// Only Academic's persisted event projection may supply actor/body facts.
type PrivateLifecycleEvent struct {
	ID               uuid.UUID  `json:"id"`
	RequestID        uuid.UUID  `json:"request_id"`
	Version          int64      `json:"version"`
	Kind             string     `json:"kind"`
	TenantID         uuid.UUID  `json:"tenant_id"`
	ParentID         uuid.UUID  `json:"parent_id"`
	ActorUserID      uuid.UUID  `json:"actor_user_id"`
	ActorKind        string     `json:"actor_kind"`
	ClassName        string     `json:"class_name"`
	StudentFirstName string     `json:"student_first_name"`
	Body             string     `json:"body"`
	InvoiceExpiresAt *time.Time `json:"invoice_expires_at"`
}
type PrivateLifecycleAcademic interface {
	PrivateLifecycle(context.Context, uuid.UUID) (PrivateLifecycleEvent, error)
}
type PrivateLifecycleStore interface {
	PrivateLifecycleMessage(context.Context, Conversation, PrivateLifecycleEvent, func(context.Context) (PrivateLifecycleEvent, error)) (Conversation, Message, bool, error)
}

func validPrivateLifecycle(in PrivateLifecycleInput, e PrivateLifecycleEvent) bool {
	if in.EventID == uuid.Nil || in.RequestID == uuid.Nil || in.TenantID == uuid.Nil || in.ParentID == uuid.Nil || e.ID != in.EventID || e.RequestID != in.RequestID || e.TenantID != in.TenantID || e.ParentID != in.ParentID || e.ActorUserID == uuid.Nil || e.Version < 0 || strings.TrimSpace(e.Body) == "" || utf8.RuneCountInString(e.Body) > 8192 {
		return false
	}
	switch e.Kind {
	case "created":
		return e.ActorKind == "parent" && e.ActorUserID == e.ParentID && e.InvoiceExpiresAt == nil
	case "approved":
		return e.ActorKind == "member"
	case "recommendation_accepted":
		return e.ActorKind == "parent" && e.ActorUserID == e.ParentID
	case "rejected":
		return e.ActorKind == "member" && e.InvoiceExpiresAt == nil
	}
	return false
}
func (s Service) SendPrivateLifecycle(ctx context.Context, in PrivateLifecycleInput) (Message, bool, error) {
	if in.EventID == uuid.Nil || in.RequestID == uuid.Nil || in.TenantID == uuid.Nil || in.ParentID == uuid.Nil {
		return Message{}, false, ErrInvalid
	}
	academic, ok := s.Academic.(PrivateLifecycleAcademic)
	if !ok || s.Tenant == nil {
		return Message{}, false, ErrUnavailable
	}
	store, ok := s.Store.(PrivateLifecycleStore)
	if !ok {
		return Message{}, false, ErrUnavailable
	}
	proof, err := academic.PrivateLifecycle(ctx, in.EventID)
	if err != nil {
		return Message{}, false, err
	}
	if !validPrivateLifecycle(in, proof) {
		return Message{}, false, ErrNotFound
	}
	tenant, err := s.Tenant.Name(ctx, in.TenantID)
	if err != nil {
		return Message{}, false, ErrUnavailable
	}
	snapshot, _ := json.Marshal(map[string]string{"class_name": proof.ClassName, "student_first_name": proof.StudentFirstName, "tenant_name": tenant})
	c := Conversation{TenantID: in.TenantID, Kind: "schedule_request", SubjectID: in.RequestID, ParentUserID: &in.ParentID, CreatedByUserID: in.ParentID, Context: snapshot}
	refresh := func(ctx context.Context) (PrivateLifecycleEvent, error) {
		current, err := academic.PrivateLifecycle(ctx, in.EventID)
		if err != nil {
			return current, err
		}
		if !validPrivateLifecycle(in, current) || current.Kind != proof.Kind || current.ActorKind != proof.ActorKind || current.ActorUserID != proof.ActorUserID || current.Version != proof.Version {
			return current, ErrNotFound
		}
		return current, nil
	}
	c, m, created, err := store.PrivateLifecycleMessage(ctx, c, proof, refresh)
	if err != nil {
		return Message{}, false, err
	}
	if created && s.Hub != nil {
		s.Hub.BroadcastMessage(c, m)
	}
	return m, created, nil
}
