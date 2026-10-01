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
var ErrUnavailable = errors.New("upstream service unavailable")
var ErrForbidden = errors.New("operation not allowed")

type SubjectContext struct {
	ID, TenantID, ParentID             uuid.UUID
	ClassName, StudentFirstName, Title string
}
type Academic interface {
	Context(context.Context, string, uuid.UUID) (SubjectContext, error)
}
type TenantInfo interface {
	Name(context.Context, uuid.UUID) (string, error)
}
type CreateInput struct {
	Kind      string
	SubjectID uuid.UUID
}

type Actor struct {
	UserID, TenantID, RoleID, MemberID uuid.UUID
	IsParent                           bool
	CanManage, CanReport               bool
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
	ID              uuid.UUID  `json:"id"`
	ConversationID  uuid.UUID  `json:"conversation_id"`
	SenderUserID    *uuid.UUID `json:"sender_user_id"`
	SenderKind      string     `json:"sender_kind"`
	Body            string     `json:"body"`
	ClientMessageID string     `json:"client_message_id"`
	CreatedAt       time.Time  `json:"created_at"`
}
type Store interface {
	Create(context.Context, Conversation) (Conversation, bool, error)
	Get(context.Context, uuid.UUID) (Conversation, error)
	List(context.Context, Actor, int, int) ([]Conversation, error)
	ListOwner(context.Context, Actor, int, int) ([]Conversation, error)
	Send(context.Context, uuid.UUID, Actor, string, string) (Message, error)
	Messages(context.Context, uuid.UUID, *uuid.UUID, int) ([]Message, error)
	Read(context.Context, uuid.UUID, uuid.UUID) error
	// Notify inserts an idempotent system message into the tenant+parent
	// notification conversation, creating that conversation when missing, and
	// reports whether the message was newly created.
	Notify(context.Context, uuid.UUID, uuid.UUID, string, string) (Conversation, Message, bool, error)
}
type Permission interface {
	CheckPermission(context.Context, string, string, string, string) (bool, error)
}
type Service struct {
	Store      Store
	Permission Permission
	Academic   Academic
	Tenant     TenantInfo
	// Hub is the optional in-memory fan-out for WS realtime events. When nil,
	// Send/Read behave exactly as before (REST only). Wired by main; unit
	// tests leave it nil.
	Hub *WSHub
}

// conversationKinds lists every conversation kind the service handles. The
// authorization gates in Service.Visible and visibleFrozen both consult it,
// so adding a kind to one and forgetting the other is not possible.
var conversationKinds = map[string]bool{"staff": true, "schedule_request": true, "report": true, "notification": true}

// Visible is the single authorization boundary shared by all conversation operations.
// Membership is checked before any remote authorization request; errors deny access.
func (s Service) allowed(ctx context.Context, a Actor, permission string) bool {
	if a.IsParent || a.TenantID == uuid.Nil || a.RoleID == uuid.Nil || a.MemberID == uuid.Nil || s.Permission == nil {
		return false
	}
	ok, err := s.Permission.CheckPermission(ctx, a.TenantID.String(), a.RoleID.String(), a.MemberID.String(), permission)
	return err == nil && ok
}

// Rights returns a copy of a with chat:manage/report:read frozen to their
// current values. WS connections capture rights once at connect time, so the
// hub fan-out consults the frozen copy instead of calling identity per event.
func (s Service) Rights(ctx context.Context, a Actor) Actor {
	a.CanManage = s.allowed(ctx, a, "chat:manage")
	a.CanReport = s.allowed(ctx, a, "report:read")
	return a
}
func (s Service) Visible(ctx context.Context, a Actor, c Conversation) bool {
	if a.UserID == uuid.Nil {
		return false
	}
	if !conversationKinds[c.Kind] {
		return false
	}
	if c.Kind != "staff" && a.IsParent {
		return c.ParentUserID != nil && *c.ParentUserID == a.UserID
	}
	if c.Kind == "notification" {
		// System notifications are parent-only: no tenant member sees them
		// through any path, regardless of chat:manage or report:read.
		return false
	}
	if a.IsParent || a.TenantID == uuid.Nil || a.TenantID != c.TenantID {
		return false
	}
	if c.Kind == "staff" && c.MemberUserID != nil && *c.MemberUserID == a.UserID {
		return true
	}
	permission := "chat:manage"
	if c.Kind == "report" {
		permission = "report:read"
	}
	return s.allowed(ctx, a, permission)
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
func (s Service) Create(ctx context.Context, a Actor, input ...CreateInput) (Conversation, bool, error) {
	if a.UserID == uuid.Nil {
		return Conversation{}, false, ErrNotFound
	}
	request := CreateInput{Kind: "staff"}
	if len(input) > 0 {
		request = input[0]
	}
	if request.Kind == "staff" {
		if len(input) > 0 && request.SubjectID != uuid.Nil && request.SubjectID != a.MemberID {
			return Conversation{}, false, ErrInvalid
		}
		if a.IsParent || a.TenantID == uuid.Nil || a.MemberID == uuid.Nil {
			return Conversation{}, false, ErrInvalid
		}
		member := a.UserID
		return s.Store.Create(ctx, Conversation{TenantID: a.TenantID, Kind: "staff", SubjectID: a.MemberID, MemberUserID: &member, CreatedByUserID: a.UserID, Context: json.RawMessage(`{}`)})
	}
	if request.SubjectID == uuid.Nil || (request.Kind != "schedule_request" && request.Kind != "report") {
		return Conversation{}, false, ErrInvalid
	}
	if request.Kind == "report" && a.IsParent {
		return Conversation{}, false, ErrNotFound
	}
	if s.Academic == nil {
		return Conversation{}, false, ErrUnavailable
	}
	source, err := s.Academic.Context(ctx, request.Kind, request.SubjectID)
	if err != nil {
		return Conversation{}, false, err
	}
	if source.ID != request.SubjectID || source.TenantID == uuid.Nil || source.ParentID == uuid.Nil {
		return Conversation{}, false, ErrUnavailable
	}
	if a.IsParent {
		if source.ParentID != a.UserID {
			return Conversation{}, false, ErrNotFound
		}
	} else {
		if a.TenantID == uuid.Nil || a.TenantID != source.TenantID {
			return Conversation{}, false, ErrNotFound
		}
		permission := "chat:manage"
		if request.Kind == "report" {
			permission = "report:read"
		}
		if !s.allowed(ctx, a, permission) {
			return Conversation{}, false, ErrNotFound
		}
	}
	if s.Tenant == nil {
		return Conversation{}, false, ErrUnavailable
	}
	name, err := s.Tenant.Name(ctx, source.TenantID)
	if err != nil {
		return Conversation{}, false, ErrUnavailable
	}
	snapshot, err := json.Marshal(map[string]string{"class_name": source.ClassName, "student_first_name": source.StudentFirstName, "report_title": source.Title, "tenant_name": name})
	if err != nil {
		return Conversation{}, false, ErrUnavailable
	}
	parent := source.ParentID
	c, created, err := s.Store.Create(ctx, Conversation{TenantID: source.TenantID, Kind: request.Kind, SubjectID: request.SubjectID, ParentUserID: &parent, CreatedByUserID: a.UserID, Context: snapshot})
	if err != nil {
		return c, created, err
	}
	// An existing row is returned only if its immutable participants still match the source.
	if c.ParentUserID == nil || *c.ParentUserID != source.ParentID {
		return Conversation{}, false, ErrNotFound
	}
	return c, created, nil
}
func (s Service) List(ctx context.Context, a Actor, page, size int) ([]Conversation, error) {
	if a.UserID == uuid.Nil || (!a.IsParent && a.TenantID == uuid.Nil) {
		return []Conversation{}, nil
	}
	var rows []Conversation
	var err error
	if a.IsParent {
		rows, err = s.Store.ListOwner(ctx, a, page, size)
	} else {
		a.CanManage = s.allowed(ctx, a, "chat:manage")
		a.CanReport = s.allowed(ctx, a, "report:read")
		rows, err = s.Store.List(ctx, a, page, size)
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
	c, err := s.Get(ctx, a, id)
	if err != nil {
		return Message{}, err
	}
	// Notification conversations are one-way: the parent who can read one
	// must not be able to reply into it.
	if c.Kind == "notification" {
		return Message{}, ErrForbidden
	}
	m, err := s.Store.Send(ctx, id, a, body, clientID)
	if err != nil {
		return Message{}, err
	}
	// Fan-out is best-effort: it never fails the REST send it follows, and
	// the hub delivers only to connections that can see this conversation.
	if s.Hub != nil {
		s.Hub.BroadcastMessage(c, m)
	}
	return m, nil
}
func (s Service) Messages(ctx context.Context, a Actor, id uuid.UUID, before *uuid.UUID, limit int) ([]Message, error) {
	if _, err := s.Get(ctx, a, id); err != nil {
		return nil, err
	}
	return s.Store.Messages(ctx, id, before, limit)
}
func (s Service) Read(ctx context.Context, a Actor, id uuid.UUID) error {
	c, err := s.Get(ctx, a, id)
	if err != nil {
		return err
	}
	if err := s.Store.Read(ctx, id, a.UserID); err != nil {
		return err
	}
	if s.Hub != nil {
		s.Hub.BroadcastRead(c, a.UserID, time.Now())
	}
	return nil
}

// NotifyInternal delivers a system message to the parent's notification
// conversation for one tenant (KEL-154). It is the entry point behind the
// internal credential boundary, never user JWTs: the caller has already been
// authenticated as a sibling service. Body and idempotency key validation
// mirror Send. The hub fan-out fires only for a newly created message, so a
// replayed idempotency key never re-broadcasts.
func (s Service) NotifyInternal(ctx context.Context, tenantID, parentUserID uuid.UUID, body, idempotencyKey string) (Message, bool, error) {
	body = strings.TrimSpace(body)
	if tenantID == uuid.Nil || parentUserID == uuid.Nil ||
		utf8.RuneCountInString(body) < 1 || utf8.RuneCountInString(body) > 2000 || strings.TrimSpace(idempotencyKey) == "" {
		return Message{}, false, ErrInvalid
	}
	c, m, created, err := s.Store.Notify(ctx, tenantID, parentUserID, body, idempotencyKey)
	if err != nil {
		return Message{}, false, err
	}
	if created && s.Hub != nil {
		s.Hub.BroadcastMessage(c, m)
	}
	return m, created, nil
}
