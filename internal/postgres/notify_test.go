package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

// TestPostgresNotificationScopeAndIdempotency is the KEL-154 risk mitigant:
// the internal write path must land the message in exactly one conversation
// scoped to (tenant, parent), be idempotent under concurrent same-key sends,
// and never leak into another tenant's or parent's view.
func TestPostgresNotificationScopeAndIdempotency(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(ctx, `TRUNCATE conversation_reads,messages,conversations CASCADE`); err != nil {
		t.Fatal(err)
	}
	store := Store{DB: db}
	svc := chat.Service{Store: store, Permission: permission(true)}

	tenant := uuid.New()
	parent := uuid.New()
	otherParent := uuid.New()
	otherTenant := uuid.New()

	// Fresh parent with no chat history: the conversation is created on
	// first notify (edge case "Parent belum pernah membuka chat").
	m1, created, err := svc.NotifyInternal(ctx, tenant, parent, "Jadwal besok pindah jam 9", "sched-1")
	if err != nil || !created {
		t.Fatalf("first notify: created=%v err=%v", created, err)
	}

	// Concurrent same-key sends converge on one message row (edge case
	// "Pesan serentak dengan idempotency key sama").
	const workers = 6
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, created, err := svc.NotifyInternal(ctx, tenant, parent, "Jadwal besok pindah jam 9", "sched-1")
			if err != nil {
				errs <- err
				return
			}
			if created {
				errs <- errDupCreate
			}
			if m.ID != m1.ID {
				errs <- errDivergentMessage
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	// A different key produces a second message in the same conversation.
	m2, created, err := svc.NotifyInternal(ctx, tenant, parent, "Anak tidak hadir hari ini", "att-1")
	if err != nil || !created || m2.ConversationID != m1.ConversationID {
		t.Fatalf("second key: m=%+v created=%v err=%v", m2, created, err)
	}

	// Parent inbox shows the notification with unread count 2.
	rows, err := svc.List(ctx, chat.Actor{UserID: parent, IsParent: true}, 1, 20)
	if err != nil || len(rows) != 1 {
		t.Fatalf("owner list: %+v %v", rows, err)
	}
	if rows[0].Kind != "notification" || rows[0].UnreadCount != 2 || rows[0].LastMessage == nil || rows[0].LastMessage.ID != m2.ID {
		t.Fatalf("owner row: %+v %+v", rows[0], rows[0].LastMessage)
	}
	if rows[0].LastMessage.SenderUserID != nil || rows[0].LastMessage.SenderKind != "system" {
		t.Fatalf("system shape: %+v", rows[0].LastMessage)
	}

	// The same parent under another tenant gets a distinct conversation
	// (one notification conversation per tenant per parent).
	m3, created, err := svc.NotifyInternal(ctx, otherTenant, parent, "Pesan dari tenant lain", "sched-1")
	if err != nil || !created || m3.ConversationID == m1.ConversationID {
		t.Fatalf("cross-tenant notify: m=%+v created=%v err=%v", m3, created, err)
	}

	// Another parent never sees this tenant's notification.
	if rows, err := svc.List(ctx, chat.Actor{UserID: otherParent, IsParent: true}, 1, 20); err != nil || len(rows) != 0 {
		t.Fatalf("other parent list: %+v %v", rows, err)
	}
	if _, err := svc.Get(ctx, chat.Actor{UserID: otherParent, IsParent: true}, m1.ConversationID); err != chat.ErrNotFound {
		t.Fatalf("other parent get: %v", err)
	}

	// Tenant members never see the notification through any path.
	manager := chat.Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	if rows, err := svc.List(ctx, manager, 1, 20); err != nil || len(rows) != 0 {
		t.Fatalf("member list: %+v %v", rows, err)
	}
	if _, err := svc.Get(ctx, manager, m1.ConversationID); err != chat.ErrNotFound {
		t.Fatalf("member get: %v", err)
	}

	// The parent cannot reply into the notification conversation.
	if _, err := svc.Send(ctx, chat.Actor{UserID: parent, IsParent: true}, m1.ConversationID, "balasan", "c1"); err != chat.ErrForbidden {
		t.Fatalf("reply: %v", err)
	}

	// Messages are readable by the owner in order.
	msgs, err := svc.Messages(ctx, chat.Actor{UserID: parent, IsParent: true}, m1.ConversationID, nil, 10)
	if err != nil || len(msgs) != 2 || msgs[1].ID != m1.ID || msgs[0].ID != m2.ID {
		t.Fatalf("messages: %+v %v", msgs, err)
	}

	// Reading clears the unread count of that conversation only; the
	// other tenant's notification stays unread.
	if err := svc.Read(ctx, chat.Actor{UserID: parent, IsParent: true}, m1.ConversationID); err != nil {
		t.Fatal(err)
	}
	rows, err = svc.List(ctx, chat.Actor{UserID: parent, IsParent: true}, 1, 20)
	if err != nil || len(rows) != 2 {
		t.Fatalf("after read: %+v %v", rows, err)
	}
	for _, row := range rows {
		switch row.TenantID {
		case tenant:
			if row.UnreadCount != 0 {
				t.Fatalf("tenant row unread=%d want 0", row.UnreadCount)
			}
		case otherTenant:
			if row.UnreadCount != 1 {
				t.Fatalf("other tenant row unread=%d want 1", row.UnreadCount)
			}
		default:
			t.Fatalf("unexpected conversation tenant %v", row.TenantID)
		}
	}

	// DB-level guarantees: exactly one notification conversation for the
	// (tenant, parent) scope, and no message outside it.
	var convCount, msgCount int
	if err := db.QueryRow(ctx, `SELECT count(*) FROM conversations WHERE kind='notification' AND tenant_id=$1 AND subject_id=$2`, tenant, parent).Scan(&convCount); err != nil || convCount != 1 {
		t.Fatalf("conversation count=%d err=%v", convCount, err)
	}
	if err := db.QueryRow(ctx, `SELECT count(*) FROM messages m JOIN conversations c ON c.id=m.conversation_id WHERE c.kind='notification' AND c.tenant_id=$1 AND c.subject_id=$2`, tenant, parent).Scan(&msgCount); err != nil || msgCount != 2 {
		t.Fatalf("message count=%d err=%v", msgCount, err)
	}
}

var errDupCreate = errors.New("concurrent same-key notify created a duplicate message")
var errDivergentMessage = errors.New("concurrent same-key notify returned a different message")
