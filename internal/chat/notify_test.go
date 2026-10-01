package chat

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// notifyStore captures the Notify call and replays a configured result.
type notifyStore struct {
	fakeStore
	c         Conversation
	m         Message
	created   bool
	calls     int
	gotTenant uuid.UUID
	gotParent uuid.UUID
	gotBody   string
	gotKey    string
}

func (f *notifyStore) Notify(_ context.Context, tenant, parent uuid.UUID, body, key string) (Conversation, Message, bool, error) {
	f.calls++
	f.gotTenant, f.gotParent, f.gotBody, f.gotKey = tenant, parent, body, key
	return f.c, f.m, f.created, nil
}

func TestNotifyInternalValidation(t *testing.T) {
	tenant, parent := uuid.New(), uuid.New()
	store := &notifyStore{}
	s := Service{Store: store}
	cases := []struct {
		name           string
		tenant, parent uuid.UUID
		body, key      string
	}{
		{"empty body", tenant, parent, "", "key"},
		{"blank body", tenant, parent, "   ", "key"},
		{"body too long", tenant, parent, strings.Repeat("é", 2001), "key"},
		{"empty key", tenant, parent, "hi", ""},
		{"blank key", tenant, parent, "hi", "  "},
		{"nil tenant", uuid.Nil, parent, "hi", "key"},
		{"nil parent", tenant, uuid.Nil, "hi", "key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := s.NotifyInternal(context.Background(), tc.tenant, tc.parent, tc.body, tc.key); err != ErrInvalid {
				t.Fatalf("err=%v want ErrInvalid", err)
			}
		})
	}
	if store.calls != 0 {
		t.Fatalf("store called %d times on invalid input", store.calls)
	}
	m, created, err := s.NotifyInternal(context.Background(), tenant, parent, "  Jadwal berubah  ", "key-1")
	if err != nil || created || m.ID != store.m.ID {
		t.Fatalf("valid: m=%+v created=%v err=%v", m, created, err)
	}
	if store.gotBody != "Jadwal berubah" || store.gotKey != "key-1" || store.gotTenant != tenant || store.gotParent != parent {
		t.Fatalf("captured tenant=%v parent=%v body=%q key=%q", store.gotTenant, store.gotParent, store.gotBody, store.gotKey)
	}
}

func notificationConv(tenant, parent uuid.UUID) Conversation {
	return Conversation{ID: uuid.New(), TenantID: tenant, Kind: "notification", SubjectID: parent, ParentUserID: &parent}
}

func TestNotificationVisibility(t *testing.T) {
	tenant, parent := uuid.New(), uuid.New()
	c := notificationConv(tenant, parent)
	s := Service{Store: &fakeStore{c: c}, Permission: permissionMatrix{"chat:manage": true, "report:read": true}}
	owner := Actor{UserID: parent, IsParent: true}
	if !s.Visible(context.Background(), owner, c) {
		t.Fatal("owner parent cannot see notification")
	}
	other := Actor{UserID: uuid.New(), IsParent: true}
	if s.Visible(context.Background(), other, c) {
		t.Fatal("other parent sees notification")
	}
	// Tenant members never see notifications, whatever their rights.
	member := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	if s.Visible(context.Background(), member, c) {
		t.Fatal("tenant member sees notification")
	}
	if _, err := s.Get(context.Background(), member, c.ID); err != ErrNotFound {
		t.Fatalf("member get: %v", err)
	}
	if _, err := s.Get(context.Background(), other, c.ID); err != ErrNotFound {
		t.Fatalf("other parent get: %v", err)
	}
	if _, err := s.Get(context.Background(), owner, c.ID); err != nil {
		t.Fatalf("owner get: %v", err)
	}
}

func TestNotificationReplyForbidden(t *testing.T) {
	tenant, parent := uuid.New(), uuid.New()
	c := notificationConv(tenant, parent)
	store := &fakeStore{c: c}
	s := Service{Store: store}
	owner := Actor{UserID: parent, IsParent: true}
	if _, err := s.Send(context.Background(), owner, c.ID, "balasan", "client-1"); err != ErrForbidden {
		t.Fatalf("parent reply: %v want ErrForbidden", err)
	}
	member := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	if _, err := s.Send(context.Background(), member, c.ID, "balasan", "client-2"); err != ErrNotFound {
		t.Fatalf("member reply: %v want ErrNotFound (invisible)", err)
	}
	if store.writes != 0 {
		t.Fatalf("store wrote %d messages into a notification conversation", store.writes)
	}
	// Reading is allowed for the owner; unread tracking is the parent's.
	if err := s.Read(context.Background(), owner, c.ID); err != nil {
		t.Fatalf("owner read: %v", err)
	}
}

func TestNotifyInternalFanoutOnlyOnCreate(t *testing.T) {
	tenant, parent := uuid.New(), uuid.New()
	c := notificationConv(tenant, parent)
	m := Message{ID: uuid.New(), ConversationID: c.ID, SenderKind: "system", Body: "Jadwal berubah"}
	hub := NewWSHub()
	cOwner, _ := hub.Register(Actor{UserID: parent, IsParent: true})
	cOther, _ := hub.Register(Actor{UserID: uuid.New(), IsParent: true})
	cMember, _ := hub.Register(Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New(), CanManage: true, CanReport: true})

	store := &notifyStore{c: c, m: m, created: true}
	s := Service{Store: store, Hub: hub}
	if _, created, err := s.NotifyInternal(context.Background(), tenant, parent, m.Body, "key-1"); err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	ev := mustRecv(t, cOwner)
	if ev.Type != WSEventMessageCreated || ev.Message == nil || ev.Message.ID != m.ID || ev.Message.SenderUserID != nil || ev.Message.SenderKind != "system" {
		t.Fatalf("owner event: %+v", ev)
	}
	for name, conn := range map[string]*WSConnection{"other parent": cOther, "tenant member": cMember} {
		if payload, ok := tryRecv(conn); ok {
			t.Fatalf("%s received leak: %s", name, payload)
		}
	}

	// A replayed idempotency key must not re-broadcast.
	store.created = false
	if _, created, err := s.NotifyInternal(context.Background(), tenant, parent, m.Body, "key-1"); err != nil || created {
		t.Fatalf("replay: created=%v err=%v", created, err)
	}
	if payload, ok := tryRecv(cOwner); ok {
		t.Fatalf("replay re-broadcast: %s", payload)
	}
}
