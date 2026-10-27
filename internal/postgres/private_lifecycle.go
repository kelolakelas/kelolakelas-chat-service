package postgres

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

func (s Store) PrivateLifecycleMessage(ctx context.Context, input chat.Conversation, proof chat.PrivateLifecycleEvent, refresh func(context.Context) (chat.PrivateLifecycleEvent, error)) (chat.Conversation, chat.Message, bool, error) {
	if input.Kind != "schedule_request" || input.ParentUserID == nil || input.MemberUserID != nil || proof.ID == uuid.Nil {
		return chat.Conversation{}, chat.Message{}, false, chat.ErrInvalid
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return chat.Conversation{}, chat.Message{}, false, err
	}
	defer tx.Rollback(ctx)
	c, err := scanConversation(tx.QueryRow(ctx, `INSERT INTO conversations(id,tenant_id,kind,subject_id,parent_user_id,member_user_id,context,created_by_user_id) VALUES($1,$2,'schedule_request',$3,$4,NULL,$5,$6) ON CONFLICT(tenant_id,kind,subject_id) DO UPDATE SET parent_user_id=conversations.parent_user_id RETURNING `+conversationColumns, uuid.New(), input.TenantID, input.SubjectID, input.ParentUserID, input.Context, input.CreatedByUserID))
	if err != nil {
		return c, chat.Message{}, false, err
	}
	if c.TenantID != input.TenantID || c.SubjectID != input.SubjectID || c.Kind != input.Kind || c.ParentUserID == nil || *c.ParentUserID != *input.ParentUserID || c.MemberUserID != nil {
		return c, chat.Message{}, false, chat.ErrNotFound
	}
	existing, err := scanMessage(tx.QueryRow(ctx, `SELECT `+messageColumns+` FROM messages WHERE source_event_id=$1`, proof.ID))
	if err == nil {
		if existing.ConversationID != c.ID || existing.SenderUserID == nil || *existing.SenderUserID != proof.ActorUserID || existing.SenderKind != proof.ActorKind {
			return c, chat.Message{}, false, chat.ErrNotFound
		}
		if err = tx.Commit(ctx); err != nil {
			return c, chat.Message{}, false, err
		}
		return c, existing, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return c, chat.Message{}, false, err
	}
	// A lock wait may cross settlement or expiry. Resolve the typed event again
	// after acquiring the room lock; transport and request contexts bound the I/O.
	fresh, err := refresh(ctx)
	if err != nil {
		return c, chat.Message{}, false, err
	}
	if fresh.ID != proof.ID || fresh.ActorUserID != proof.ActorUserID || fresh.ActorKind != proof.ActorKind || fresh.RequestID != c.SubjectID || fresh.TenantID != c.TenantID || fresh.ParentID != *c.ParentUserID {
		return c, chat.Message{}, false, chat.ErrNotFound
	}
	if fresh.InvoiceExpiresAt != nil {
		var active bool
		if err = tx.QueryRow(ctx, "SELECT $1::timestamptz > clock_timestamp()", fresh.InvoiceExpiresAt).Scan(&active); err != nil {
			return c, chat.Message{}, false, err
		}
		if !active {
			return c, chat.Message{}, false, chat.ErrPaymentLinkInactive
		}
	}
	key := "private-event:" + proof.ID.String()
	m, err := scanMessage(tx.QueryRow(ctx, `INSERT INTO messages(id,conversation_id,sender_user_id,sender_kind,body,client_message_id,source_event_id) SELECT $1,$2,$3,$4,$5,$6,$7 WHERE $8::timestamptz IS NULL OR $8::timestamptz>clock_timestamp() ON CONFLICT DO NOTHING RETURNING `+messageColumns, uuid.New(), c.ID, proof.ActorUserID, proof.ActorKind, fresh.Body, key, proof.ID, fresh.InvoiceExpiresAt))
	if errors.Is(err, pgx.ErrNoRows) {
		return c, chat.Message{}, false, chat.ErrPaymentLinkInactive
	}
	if err != nil {
		return c, chat.Message{}, false, err
	}
	if _, err = tx.Exec(ctx, "UPDATE conversations SET last_message_at=GREATEST(COALESCE(last_message_at,$2),$2) WHERE id=$1", c.ID, m.CreatedAt); err != nil {
		return c, chat.Message{}, false, err
	}
	if err = tx.Commit(ctx); err != nil {
		return c, chat.Message{}, false, err
	}
	return c, m, true, nil
}
