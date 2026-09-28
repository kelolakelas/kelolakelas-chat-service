package chathttp

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type stubStore struct{ c chat.Conversation }

func (s stubStore) Create(context.Context, chat.Actor) (chat.Conversation, bool, error) {
	return s.c, true, nil
}
func (s stubStore) Get(context.Context, uuid.UUID) (chat.Conversation, error) { return s.c, nil }
func (s stubStore) List(context.Context, chat.Actor, int, int) ([]chat.Conversation, error) {
	return []chat.Conversation{s.c}, nil
}
func (s stubStore) ListOwner(context.Context, chat.Actor, int, int) ([]chat.Conversation, error) {
	return []chat.Conversation{}, nil
}
func (s stubStore) Send(context.Context, uuid.UUID, chat.Actor, string, string) (chat.Message, error) {
	return chat.Message{}, nil
}
func (s stubStore) Messages(context.Context, uuid.UUID, *uuid.UUID, int) ([]chat.Message, error) {
	return []chat.Message{}, nil
}
func (s stubStore) Read(context.Context, uuid.UUID, uuid.UUID) error { return nil }

type down struct{}

func (down) CheckPermission(context.Context, string, string, string, string) (bool, error) {
	return false, errors.New("private-host:5432 unavailable")
}
func signed(user, tenant, role, member string, parent bool) string {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims{UserID: user, TenantID: tenant, RoleID: role, MemberID: member, IsParent: parent, RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}})
	value, _ := token.SignedString([]byte("secret"))
	return "Bearer " + value
}
func TestHandlerSecurity(t *testing.T) {
	tenant := uuid.New()
	owner := uuid.New()
	id := uuid.New()
	h := Handler{Secret: "secret", Service: chat.Service{Store: stubStore{c: chat.Conversation{ID: id, TenantID: tenant, Kind: "staff", MemberUserID: &owner}}, Permission: down{}}}
	url := "/api/v1/chat/conversations/" + id.String()
	member := uuid.New()
	auth := signed(member.String(), tenant.String(), uuid.NewString(), uuid.NewString(), false)
	cases := []struct {
		name, method, path, token, body string
		want                            int
	}{
		{"missing user", "GET", url, signed("", tenant.String(), "", "", false), "", 401},
		{"missing tenant", "GET", url, signed(member.String(), "", "", "", false), "", 401},
		{"not visible", "GET", url, auth, "", 404},
		{"permission unavailable list", "GET", "/api/v1/chat/conversations", auth, "", 200},
		{"permission unavailable send", "POST", url + "/messages", auth, `{"body":"hi","client_message_id":"k"}`, 404},
		{"invalid id", "GET", "/api/v1/chat/conversations/not-uuid", auth, "", 400},
		{"empty body", "POST", url + "/messages", signed(owner.String(), tenant.String(), "", "", false), `{"body":" ","client_message_id":"k"}`, 400},
		{"parent invisible", "GET", url, signed(uuid.NewString(), "", "", "", true), "", 404},
		{"parent empty list", "GET", "/api/v1/chat/conversations", signed(uuid.NewString(), "", "", "", true), "", 200},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, bytes.NewBufferString(tc.body))
			req.Header.Set("Authorization", tc.token)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.want {
				t.Fatalf("code=%d body=%s", rec.Code, rec.Body.String())
			}
			if strings.Contains(rec.Body.String(), "private-host") {
				t.Fatal("internal error leaked")
			}
		})
	}
}
