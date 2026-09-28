package chat

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type fakePermission struct {
	allowed bool
	err     error
}

func (p fakePermission) CheckPermission(_ context.Context, _, _, _, _ string) (bool, error) {
	return p.allowed, p.err
}

type fakeStore struct {
	c      Conversation
	m      Message
	writes int
}

func (f *fakeStore) Create(_ context.Context, _ Actor) (Conversation, bool, error) {
	return f.c, true, nil
}
func (f *fakeStore) Get(_ context.Context, _ uuid.UUID) (Conversation, error) { return f.c, nil }
func (f *fakeStore) List(_ context.Context, _ Actor, _, _ int) ([]Conversation, error) {
	return []Conversation{f.c}, nil
}
func (f *fakeStore) ListOwner(_ context.Context, _ Actor, _, _ int) ([]Conversation, error) {
	return []Conversation{f.c}, nil
}
func (f *fakeStore) Send(_ context.Context, id uuid.UUID, a Actor, body, key string) (Message, error) {
	if f.m.ID != uuid.Nil && f.m.ClientMessageID == key {
		return f.m, nil
	}
	f.writes++
	f.m = Message{ID: uuid.New(), ConversationID: id, SenderUserID: a.UserID, Body: body, ClientMessageID: key}
	return f.m, nil
}
func (f *fakeStore) Messages(_ context.Context, _ uuid.UUID, _ *uuid.UUID, _ int) ([]Message, error) {
	return nil, nil
}
func (f *fakeStore) Read(_ context.Context, _, _ uuid.UUID) error { return nil }
func TestVisible(t *testing.T) {
	tenant := uuid.New()
	owner := Actor{UserID: uuid.New(), TenantID: tenant}
	member := owner.UserID
	c := Conversation{ID: uuid.New(), TenantID: tenant, Kind: "staff", MemberUserID: &member}
	manager := Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	cases := []struct {
		name string
		a    Actor
		p    fakePermission
		want bool
	}{
		{"owner", owner, fakePermission{}, true}, {"manager", manager, fakePermission{allowed: true}, true}, {"denied", manager, fakePermission{}, false}, {"identity down", manager, fakePermission{err: errors.New("unavailable")}, false},
		{"wrong tenant", Actor{UserID: manager.UserID, TenantID: uuid.New(), RoleID: manager.RoleID, MemberID: manager.MemberID}, fakePermission{allowed: true}, false},
		{"parent", Actor{UserID: uuid.New(), TenantID: tenant, IsParent: true, RoleID: manager.RoleID, MemberID: manager.MemberID}, fakePermission{allowed: true}, false},
		{"missing membership", Actor{UserID: manager.UserID, TenantID: tenant, RoleID: manager.RoleID}, fakePermission{allowed: true}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := Service{Permission: tc.p}
			if got := s.Visible(context.Background(), tc.a, c); got != tc.want {
				t.Fatalf("visible=%v want %v", got, tc.want)
			}
		})
	}
}
func TestSendValidationAndIdempotency(t *testing.T) {
	a := Actor{UserID: uuid.New(), TenantID: uuid.New()}
	member := a.UserID
	store := &fakeStore{c: Conversation{ID: uuid.New(), TenantID: a.TenantID, Kind: "staff", MemberUserID: &member}}
	s := Service{Store: store}
	for _, body := range []string{"", "  ", strings.Repeat("é", 2001)} {
		if _, err := s.Send(context.Background(), a, store.c.ID, body, "key"); err != ErrInvalid {
			t.Fatalf("body %q: %v", body, err)
		}
	}
	if _, err := s.Send(context.Background(), a, store.c.ID, "hi", ""); err != ErrInvalid {
		t.Fatal(err)
	}
	first, err := s.Send(context.Background(), a, store.c.ID, "  hi  ", "key")
	if err != nil || first.Body != "hi" {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, err := s.Send(context.Background(), a, store.c.ID, "other", "key")
	if err != nil || second.ID != first.ID || store.writes != 1 {
		t.Fatalf("duplicate: %+v %v writes=%d", second, err, store.writes)
	}
}
