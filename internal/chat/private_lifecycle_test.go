package chat

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"strings"
	"testing"
)

type lifecycleAcademic struct {
	Academic
	proof PrivateLifecycleEvent
	err   error
	calls int
}

func (a *lifecycleAcademic) PrivateLifecycle(context.Context, uuid.UUID) (PrivateLifecycleEvent, error) {
	a.calls++
	return a.proof, a.err
}

type lifecycleStore struct {
	Store
	calls int
}

func (s *lifecycleStore) PrivateLifecycleMessage(ctx context.Context, c Conversation, e PrivateLifecycleEvent, refresh func(context.Context) (PrivateLifecycleEvent, error)) (Conversation, Message, bool, error) {
	s.calls++
	fresh, err := refresh(ctx)
	return c, Message{ID: uuid.New(), SenderUserID: &fresh.ActorUserID, SenderKind: fresh.ActorKind}, true, err
}
func TestPrivateLifecycleUsesPersistedActorAndScope(t *testing.T) {
	in := PrivateLifecycleInput{EventID: uuid.New(), RequestID: uuid.New(), TenantID: uuid.New(), ParentID: uuid.New()}
	good := PrivateLifecycleEvent{ID: in.EventID, RequestID: in.RequestID, TenantID: in.TenantID, ParentID: in.ParentID, ActorUserID: in.ParentID, ActorKind: "parent", Kind: "created", Body: "Request"}
	for _, name := range []string{"valid", "wrong-event", "wrong-request", "wrong-tenant", "wrong-parent", "wrong-actor", "wrong-kind", "empty", "too-large", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			e := good
			switch name {
			case "wrong-event":
				e.ID = uuid.New()
			case "wrong-request":
				e.RequestID = uuid.New()
			case "wrong-tenant":
				e.TenantID = uuid.New()
			case "wrong-parent":
				e.ParentID = uuid.New()
			case "wrong-actor":
				e.ActorUserID = uuid.New()
			case "wrong-kind":
				e.ActorKind = "system"
			case "empty":
				e.Body = ""
			case "too-large":
				e.Body = strings.Repeat("x", 8193)
			}
			a := &lifecycleAcademic{proof: e}
			if name == "unavailable" {
				a.err = ErrUnavailable
			}
			store := &lifecycleStore{}
			s := Service{Store: store, Academic: a, Tenant: tenantReader{}}
			m, created, err := s.SendPrivateLifecycle(context.Background(), in)
			if name == "valid" {
				if err != nil || !created || m.SenderKind != "parent" || *m.SenderUserID != in.ParentID || a.calls != 2 {
					t.Fatalf("message %+v calls%d err%v", m, a.calls, err)
				}
			} else if err == nil || store.calls != 0 {
				t.Fatalf("bad proof written: %v", err)
			}
		})
	}
	// Members cannot inject the reserved internal event key through public Send.
	if _, err := (Service{}).Send(context.Background(), Actor{}, uuid.New(), "hello", "private-event:"+uuid.NewString()); !errors.Is(err, ErrInvalid) {
		t.Fatalf("reserved key %v", err)
	}
}
