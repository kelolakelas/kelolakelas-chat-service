package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type Store struct{ DB *pgxpool.Pool }
type scanner interface{ Scan(...any) error }

const conversationColumns = `id,tenant_id,kind,subject_id,parent_user_id,member_user_id,context,created_by_user_id,created_at,last_message_at`

func scanConversation(row scanner) (chat.Conversation, error) {
	var c chat.Conversation
	err := row.Scan(&c.ID, &c.TenantID, &c.Kind, &c.SubjectID, &c.ParentUserID, &c.MemberUserID, &c.Context, &c.CreatedByUserID, &c.CreatedAt, &c.LastMessageAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, chat.ErrNotFound
	}
	return c, err
}
func scanMessage(row scanner) (chat.Message, error) {
	var m chat.Message
	err := row.Scan(&m.ID, &m.ConversationID, &m.SenderUserID, &m.SenderKind, &m.Body, &m.ClientMessageID, &m.CreatedAt)
	return m, err
}

const messageColumns = `id,conversation_id,sender_user_id,sender_kind,body,client_message_id,created_at`

func (s Store) Create(ctx context.Context, input chat.Conversation) (chat.Conversation, bool, error) {
	c, err := scanConversation(s.DB.QueryRow(ctx, `INSERT INTO conversations (id,tenant_id,kind,subject_id,parent_user_id,member_user_id,context,created_by_user_id)
 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (tenant_id,kind,subject_id) DO NOTHING RETURNING `+conversationColumns,
		uuid.New(), input.TenantID, input.Kind, input.SubjectID, input.ParentUserID, input.MemberUserID, input.Context, input.CreatedByUserID))
	if err == nil {
		return c, true, nil
	}
	if !errors.Is(err, chat.ErrNotFound) {
		return c, false, err
	}
	c, err = scanConversation(s.DB.QueryRow(ctx, `SELECT `+conversationColumns+` FROM conversations WHERE tenant_id=$1 AND kind=$2 AND subject_id=$3`, input.TenantID, input.Kind, input.SubjectID))
	return c, false, err
}
func (s Store) Get(ctx context.Context, id uuid.UUID) (chat.Conversation, error) {
	return scanConversation(s.DB.QueryRow(ctx, `SELECT `+conversationColumns+` FROM conversations WHERE id=$1`, id))
}
func (s Store) List(ctx context.Context, a chat.Actor, page, size int) ([]chat.Conversation, error) {
	return s.list(ctx, `c.tenant_id=$1 AND ((c.kind='staff' AND (c.member_user_id=$2 OR $3::boolean)) OR (c.kind='schedule_request' AND $3::boolean) OR (c.kind='report' AND $4::boolean))`, []any{a.TenantID, a.UserID, a.CanManage, a.CanReport}, a.UserID, page, size)
}
func (s Store) ListOwner(ctx context.Context, a chat.Actor, page, size int) ([]chat.Conversation, error) {
	return s.list(ctx, `c.parent_user_id=$1 AND c.kind IN ('schedule_request','report','notification')`, []any{a.UserID}, a.UserID, page, size)
}
func (s Store) list(ctx context.Context, where string, args []any, user uuid.UUID, page, size int) ([]chat.Conversation, error) {
	// Authorization predicate precedes pagination, preventing sparse pages and leaks.
	userPos := len(args) + 1
	args = append(args, user)
	limitPos := len(args) + 1
	args = append(args, size, (page-1)*size)
	sql := fmt.Sprintf(`SELECT c.id,c.tenant_id,c.kind,c.subject_id,c.parent_user_id,c.member_user_id,c.context,c.created_by_user_id,c.created_at,c.last_message_at,
 m.id,m.conversation_id,m.sender_user_id,m.sender_kind,m.body,m.client_message_id,m.created_at,
 (SELECT count(*) FROM messages unread LEFT JOIN conversation_reads r ON r.conversation_id=c.id AND r.user_id=$%d
 WHERE unread.conversation_id=c.id AND unread.sender_user_id IS DISTINCT FROM $%d::uuid AND (r.last_read_at IS NULL OR unread.created_at>r.last_read_at))
 FROM conversations c LEFT JOIN LATERAL (SELECT id,conversation_id,sender_user_id,sender_kind,body,client_message_id,created_at FROM messages WHERE conversation_id=c.id ORDER BY created_at DESC,id DESC LIMIT 1) m ON true
 WHERE %s ORDER BY c.last_message_at DESC NULLS LAST,c.created_at DESC,c.id DESC LIMIT $%d OFFSET $%d`, userPos, userPos, where, limitPos, limitPos+1)
	return s.listQuery(ctx, sql, args...)
}
func (s Store) listQuery(ctx context.Context, sql string, args ...any) ([]chat.Conversation, error) {
	rows, err := s.DB.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []chat.Conversation{}
	for rows.Next() {
		var c chat.Conversation
		var m chat.Message
		var mid *uuid.UUID
		var conversationID, senderID *uuid.UUID
		var kind, body, clientID *string
		var created *time.Time
		err = rows.Scan(&c.ID, &c.TenantID, &c.Kind, &c.SubjectID, &c.ParentUserID, &c.MemberUserID, &c.Context, &c.CreatedByUserID, &c.CreatedAt, &c.LastMessageAt, &mid, &conversationID, &senderID, &kind, &body, &clientID, &created, &c.UnreadCount)
		if err != nil {
			return nil, err
		}
		if mid != nil {
			m.ID = *mid
			m.ConversationID = *conversationID
			// senderID is NULL for system messages; keep the pointer so the
			// JSON shape matches the stored row.
			m.SenderUserID = senderID
			m.SenderKind = *kind
			m.Body = *body
			m.ClientMessageID = *clientID
			m.CreatedAt = *created
			c.LastMessage = &m
		}
		result = append(result, c)
	}
	return result, rows.Err()
}
func (s Store) Send(ctx context.Context, id uuid.UUID, a chat.Actor, body, clientID string) (chat.Message, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return chat.Message{}, err
	}
	defer tx.Rollback(ctx)
	sender := "member"
	if a.IsParent {
		sender = "parent"
	}
	m, err := scanMessage(tx.QueryRow(ctx, `INSERT INTO messages(id,conversation_id,sender_user_id,sender_kind,body,client_message_id)
 VALUES ($1,$2,$3,$6,$4,$5) ON CONFLICT (conversation_id,sender_user_id,client_message_id) DO NOTHING RETURNING `+messageColumns, uuid.New(), id, a.UserID, body, clientID, sender))
	if errors.Is(err, pgx.ErrNoRows) {
		m, err = scanMessage(tx.QueryRow(ctx, `SELECT `+messageColumns+` FROM messages WHERE conversation_id=$1 AND sender_user_id=$2 AND client_message_id=$3`, id, a.UserID, clientID))
	} else if err == nil {
		_, err = tx.Exec(ctx, `UPDATE conversations SET last_message_at=GREATEST(COALESCE(last_message_at,$2),$2) WHERE id=$1`, id, m.CreatedAt)
	}
	if err != nil {
		return chat.Message{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return chat.Message{}, err
	}
	return m, nil
}
func (s Store) Messages(ctx context.Context, id uuid.UUID, before *uuid.UUID, limit int) ([]chat.Message, error) {
	var cursor any
	cursorID := uuid.Nil
	if before != nil {
		cursorID = *before
		err := s.DB.QueryRow(ctx, `SELECT created_at FROM messages WHERE id=$1 AND conversation_id=$2`, *before, id).Scan(&cursor)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, chat.ErrInvalid
		}
		if err != nil {
			return nil, err
		}
	}
	rows, err := s.DB.Query(ctx, `SELECT `+messageColumns+` FROM messages WHERE conversation_id=$1 AND ($2::timestamptz IS NULL OR (created_at,id)<($2::timestamptz,$3::uuid)) ORDER BY created_at DESC,id DESC LIMIT $4`, id, cursor, cursorID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []chat.Message{}
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, m)
	}
	return result, rows.Err()
}
func (s Store) Read(ctx context.Context, id, user uuid.UUID) error {
	_, err := s.DB.Exec(ctx, `INSERT INTO conversation_reads(conversation_id,user_id,last_read_at) VALUES ($1,$2,now()) ON CONFLICT (conversation_id,user_id) DO UPDATE SET last_read_at=GREATEST(conversation_reads.last_read_at,excluded.last_read_at)`, id, user)
	return err
}

// Notify implements the system-message write path (KEL-154). Everything it
// needs to be safe lives inside one transaction: the notification conversation
// is scoped to (tenant_id, parent_user_id) by the unique constraint, the
// message insert is idempotent through the system partial unique index, and
// both rely on ON CONFLICT so concurrent senders converge on one row without
// erroring. The DB-level CHECK constraints (participants, subject_id equality)
// are the last line of defense against a mis-scoped write.
func (s Store) Notify(ctx context.Context, tenantID, parentUserID uuid.UUID, body, idempotencyKey string) (chat.Conversation, chat.Message, bool, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	defer tx.Rollback(ctx)
	c, err := scanConversation(tx.QueryRow(ctx, `INSERT INTO conversations (id,tenant_id,kind,subject_id,parent_user_id,member_user_id,context,created_by_user_id)
 VALUES ($1,$2,'notification',$3,$3,NULL,'{}'::jsonb,$3)
 ON CONFLICT (tenant_id,kind,subject_id) DO UPDATE SET parent_user_id=conversations.parent_user_id
 RETURNING `+conversationColumns, uuid.New(), tenantID, parentUserID))
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	// Defense in depth against a concurrent or historical row that violates
	// the one-parent scoping: never write into it.
	if c.Kind != "notification" || c.ParentUserID == nil || *c.ParentUserID != parentUserID || c.SubjectID != parentUserID {
		return chat.Conversation{}, chat.Message{}, false, chat.ErrInvalid
	}
	m, err := scanMessage(tx.QueryRow(ctx, `INSERT INTO messages(id,conversation_id,sender_user_id,sender_kind,body,client_message_id)
 VALUES ($1,$2,NULL,'system',$3,$4)
 ON CONFLICT DO NOTHING RETURNING `+messageColumns, uuid.New(), c.ID, body, idempotencyKey))
	if errors.Is(err, pgx.ErrNoRows) {
		m, err = scanMessage(tx.QueryRow(ctx, `SELECT `+messageColumns+` FROM messages WHERE conversation_id=$1 AND sender_kind='system' AND client_message_id=$2`, c.ID, idempotencyKey))
		if err != nil {
			return chat.Conversation{}, chat.Message{}, false, err
		}
		if err = tx.Commit(ctx); err != nil {
			return chat.Conversation{}, chat.Message{}, false, err
		}
		return c, m, false, nil
	}
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	_, err = tx.Exec(ctx, `UPDATE conversations SET last_message_at=GREATEST(COALESCE(last_message_at,$2),$2) WHERE id=$1`, c.ID, m.CreatedAt)
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	return c, m, true, nil
}
