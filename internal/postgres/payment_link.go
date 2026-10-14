package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

var _ chat.PaymentLinkStore = Store{}

func (s Store) ScheduleRequestPaymentLink(ctx context.Context, input chat.Conversation, body, key string, expiresAt time.Time) (chat.Conversation, chat.Message, bool, error) {
	if input.Kind != "schedule_request" || input.TenantID == uuid.Nil || input.SubjectID == uuid.Nil || input.ParentUserID == nil || *input.ParentUserID == uuid.Nil || input.MemberUserID != nil {
		return chat.Conversation{}, chat.Message{}, false, chat.ErrInvalid
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	defer tx.Rollback(ctx)
	// The no-op conflict update locks the existing conversation for the whole
	// write, serializing concurrent first sends without changing its snapshot.
	c, err := scanConversation(tx.QueryRow(ctx, `INSERT INTO conversations(id,tenant_id,kind,subject_id,parent_user_id,member_user_id,context,created_by_user_id)
 VALUES ($1,$2,'schedule_request',$3,$4,NULL,$5,$6)
 ON CONFLICT (tenant_id,kind,subject_id) DO UPDATE SET parent_user_id=conversations.parent_user_id
 RETURNING `+conversationColumns, uuid.New(), input.TenantID, input.SubjectID, input.ParentUserID, input.Context, input.CreatedByUserID))
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	if c.Kind != input.Kind || c.TenantID != input.TenantID || c.SubjectID != input.SubjectID || c.ParentUserID == nil || *c.ParentUserID != *input.ParentUserID || c.MemberUserID != nil {
		return chat.Conversation{}, chat.Message{}, false, chat.ErrNotFound
	}
	// Use wall-clock time after acquiring the conversation lock; transaction
	// timestamps are frozen before a lock wait and would allow expired writes.
	var active bool
	if err = tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, expiresAt).Scan(&active); err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	if expiresAt.IsZero() || !active {
		return chat.Conversation{}, chat.Message{}, false, chat.ErrPaymentLinkInactive
	}
	m, err := scanMessage(tx.QueryRow(ctx, `INSERT INTO messages(id,conversation_id,sender_user_id,sender_kind,body,client_message_id)
 SELECT $1,$2,NULL,'system',$3,$4 WHERE $5::timestamptz > clock_timestamp() ON CONFLICT DO NOTHING RETURNING `+messageColumns, uuid.New(), c.ID, body, key, expiresAt))
	if err == pgx.ErrNoRows {
		if clockErr := tx.QueryRow(ctx, `SELECT $1::timestamptz > clock_timestamp()`, expiresAt).Scan(&active); clockErr != nil {
			return chat.Conversation{}, chat.Message{}, false, clockErr
		}
		if !active {
			return chat.Conversation{}, chat.Message{}, false, chat.ErrPaymentLinkInactive
		}
	}
	created := err == nil
	if errors.Is(err, pgx.ErrNoRows) {
		m, err = scanMessage(tx.QueryRow(ctx, `SELECT `+messageColumns+` FROM messages WHERE conversation_id=$1 AND sender_kind='system' AND client_message_id=$2`, c.ID, key))
	}
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	if created {
		_, err = tx.Exec(ctx, `UPDATE conversations SET last_message_at=GREATEST(COALESCE(last_message_at,$2),$2) WHERE id=$1`, c.ID, m.CreatedAt)
		if err != nil {
			return chat.Conversation{}, chat.Message{}, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	return c, m, created, nil
}
