package chat

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
)

var ErrNotFound = errors.New("conversation not found")
var ErrInvalid = errors.New("invalid input")

type Actor struct {
	UserID, TenantID, RoleID, MemberID uuid.UUID
	IsParent                           bool
}
type Conversation struct {
	ID              uuid.UUID       `json:"id"`
	TenantID        uuid.UUID       `json:"tenant_id"`
	Kind            string          `json:"kind"`
	SubjectID       uuid.UUID       `json:"subject_id"`
	ParentUserID    *uuid.UUID      `json:"parent_user_id"`
	MemberUserID    *uuid.UUID      `json:"member_user_id"`
	Context         json.RawMessage `json:"context"`
	CreatedByUserID uuid.UUID       `json:"created_by_user_id"`
	CreatedAt       time.Time       `json:"created_at"`
	LastMessageAt   *time.Time      `json:"last_message_at"`
	LastMessage     *Message        `json:"last_message,omitempty"`
	UnreadCount     int             `json:"unread_count"`
}
type Message struct {
	ID              uuid.UUID `json:"id"`
	ConversationID  uuid.UUID `json:"conversation_id"`
	SenderUserID    uuid.UUID `json:"sender_user_id"`
	SenderKind      string    `json:"sender_kind"`
	Body            string    `json:"body"`
	ClientMessageID string    `json:"client_message_id"`
	CreatedAt       time.Time `json:"created_at"`
}
type Store interface {
	Create(context.Context, Actor) (Conversation, bool, error)
	Get(context.Context, uuid.UUID) (Conversation, error)
	List(context.Context, Actor, int, int) ([]Conversation, error)
	ListOwner(context.Context, Actor, int, int) ([]Conversation, error)
	Send(context.Context, uuid.UUID, Actor, string, string) (Message, error)
	Messages(context.Context, uuid.UUID, *uuid.UUID, int) ([]Message, error)
	Read(context.Context, uuid.UUID, uuid.UUID) error
}
type Permission interface {
	CheckPermission(context.Context, string, string, string, string) (bool, error)
}
type Service struct {
	Store      Store
	Permission Permission
}

// Visible is the single authorization boundary shared by all conversation operations.
// Membership is checked before any remote authorization request; errors deny access.
func (s Service) Visible(ctx context.Context, a Actor, c Conversation) bool {
	if a.IsParent || a.UserID == uuid.Nil || a.TenantID == uuid.Nil || a.TenantID != c.TenantID || c.Kind != "staff" {
		return false
	}
	if c.MemberUserID != nil && *c.MemberUserID == a.UserID {
		return true
	}
	if a.RoleID == uuid.Nil || a.MemberID == uuid.Nil || s.Permission == nil {
		return false
	}
	ok, err := s.Permission.CheckPermission(ctx, a.TenantID.String(), a.RoleID.String(), a.MemberID.String(), "chat:manage")
	return err == nil && ok
}
func (s Service) Get(ctx context.Context, a Actor, id uuid.UUID) (Conversation, error) {
	c, err := s.Store.Get(ctx, id)
	if err != nil {
		return c, err
	}
	if !s.Visible(ctx, a, c) {
		return Conversation{}, ErrNotFound
	}
	return c, nil
}
func (s Service) Create(ctx context.Context, a Actor) (Conversation, bool, error) {
	if a.IsParent || a.TenantID == uuid.Nil || a.MemberID == uuid.Nil || a.UserID == uuid.Nil {
		return Conversation{}, false, ErrInvalid
	}
	return s.Store.Create(ctx, a)
}
func (s Service) List(ctx context.Context, a Actor, page, size int) ([]Conversation, error) {
	if a.IsParent || a.TenantID == uuid.Nil {
		return []Conversation{}, nil
	}
	// Owner rows are always visible. A manager can see every row, but identity
	// failure is treated as a denial rather than a server error.
	manager := false
	if a.RoleID != uuid.Nil && a.MemberID != uuid.Nil && s.Permission != nil {
		ok, err := s.Permission.CheckPermission(ctx, a.TenantID.String(), a.RoleID.String(), a.MemberID.String(), "chat:manage")
		manager = err == nil && ok
	}
	var rows []Conversation
	var err error
	if manager {
		rows, err = s.Store.List(ctx, a, page, size)
	} else {
		rows, err = s.Store.ListOwner(ctx, a, page, size)
	}
	if err != nil {
		return nil, err
	}
	// Defense in depth: every returned row uses the same visibility rule.
	result := make([]Conversation, 0, len(rows))
	for _, row := range rows {
		if s.Visible(ctx, a, row) {
			result = append(result, row)
		}
	}
	return result, nil
}
func (s Service) Send(ctx context.Context, a Actor, id uuid.UUID, body, clientID string) (Message, error) {
	body = strings.TrimSpace(body)
	if utf8.RuneCountInString(body) < 1 || utf8.RuneCountInString(body) > 2000 || strings.TrimSpace(clientID) == "" {
		return Message{}, ErrInvalid
	}
	if _, err := s.Get(ctx, a, id); err != nil {
		return Message{}, err
	}
	return s.Store.Send(ctx, id, a, body, clientID)
}
func (s Service) Messages(ctx context.Context, a Actor, id uuid.UUID, before *uuid.UUID, limit int) ([]Message, error) {
	if _, err := s.Get(ctx, a, id); err != nil {
		return nil, err
	}
	return s.Store.Messages(ctx, id, before, limit)
}
func (s Service) Read(ctx context.Context, a Actor, id uuid.UUID) error {
	if _, err := s.Get(ctx, a, id); err != nil {
		return err
	}
	return s.Store.Read(ctx, id, a.UserID)
}
