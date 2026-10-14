package postgres

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

func TestPostgresPaymentLinkAtomicScopeAndRetries(t *testing.T) {
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
	store := Store{DB: db}
	parent := uuid.New()
	c := chat.Conversation{TenantID: uuid.New(), Kind: "schedule_request", SubjectID: uuid.New(), ParentUserID: &parent, CreatedByUserID: parent, Context: []byte(`{"tenant_name":"School"}`)}
	defer db.Exec(ctx, `DELETE FROM conversations WHERE tenant_id=$1`, c.TenantID)
	key := "private-payment:" + c.SubjectID.String() + ":" + uuid.NewString()
	const workers = 8
	var wg sync.WaitGroup
	var createdCount atomic.Int32
	results := make(chan chat.Message, workers)
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, m, created, err := store.ScheduleRequestPaymentLink(ctx, c, "https://pay.example/x", key, time.Now().Add(time.Hour))
			if err != nil {
				errs <- err
				return
			}
			if created {
				createdCount.Add(1)
			}
			results <- m
		}()
	}
	wg.Wait()
	close(errs)
	close(results)
	for err := range errs {
		t.Fatal(err)
	}
	if createdCount.Load() != 1 {
		t.Fatalf("created=%d", createdCount.Load())
	}
	var first chat.Message
	for m := range results {
		if first.ID == uuid.Nil {
			first = m
		}
		if m.ID != first.ID || m.ConversationID != first.ConversationID || m.SenderUserID != nil || m.SenderKind != "system" {
			t.Fatalf("divergent message %+v", m)
		}
	}
	before, err := store.Get(ctx, first.ConversationID)
	if err != nil {
		t.Fatal(err)
	}
	_, retry, created, err := store.ScheduleRequestPaymentLink(ctx, c, "changed URL", key, time.Now().Add(time.Hour))
	after, getErr := store.Get(ctx, first.ConversationID)
	if err != nil || getErr != nil || created || retry.ID != first.ID || retry.Body != first.Body || before.LastMessageAt == nil || after.LastMessageAt == nil || !before.LastMessageAt.Equal(*after.LastMessageAt) {
		t.Fatalf("retry=%+v created=%v err=%v get=%v", retry, created, err, getErr)
	}
	wrong := uuid.New()
	bad := c
	bad.ParentUserID = &wrong
	if _, _, _, err = store.ScheduleRequestPaymentLink(ctx, bad, "wrong parent", key+"other", time.Now().Add(time.Hour)); !errors.Is(err, chat.ErrNotFound) {
		t.Fatalf("frozen parent err=%v", err)
	}
	msgs, err := store.Messages(ctx, first.ConversationID, nil, 100)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("messages=%+v err=%v", msgs, err)
	}
	svc := chat.Service{Store: store, Permission: permission(true)}
	for _, a := range []chat.Actor{{UserID: wrong, IsParent: true}, {UserID: uuid.New(), TenantID: uuid.New(), RoleID: uuid.New(), MemberID: uuid.New()}} {
		if _, err := svc.Get(ctx, a, first.ConversationID); !errors.Is(err, chat.ErrNotFound) {
			t.Fatalf("isolation err=%v", err)
		}
	}
	if _, err := svc.Get(ctx, chat.Actor{UserID: parent, IsParent: true}, first.ConversationID); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Get(ctx, chat.Actor{UserID: uuid.New(), TenantID: c.TenantID, RoleID: uuid.New(), MemberID: uuid.New()}, first.ConversationID); err != nil {
		t.Fatal(err)
	}
	// A new transaction is a distinct system message in the same request chat.
	_, next, created, err := store.ScheduleRequestPaymentLink(ctx, c, "next invoice", key+"next", time.Now().Add(time.Hour))
	if err != nil || !created || next.ConversationID != first.ConversationID || next.ID == first.ID {
		t.Fatalf("next=%+v %v %v", next, created, err)
	}
}
