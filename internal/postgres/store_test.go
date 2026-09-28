package postgres

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

type permission bool

func (p permission) CheckPermission(_ context.Context, _, _, _, _ string) (bool, error) {
	return bool(p), nil
}
func TestPostgresStaffIsolationAndConcurrency(t *testing.T) {
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
	_, err = db.Exec(ctx, `TRUNCATE conversation_reads,messages,conversations CASCADE`)
	if err != nil {
		t.Fatal(err)
	}
	store := Store{DB: db}
	tenant := uuid.New()
	otherTenant := uuid.New()
	owner := chat.Actor{UserID: uuid.New(), TenantID: tenant, MemberID: uuid.New()}
	manager := chat.Actor{UserID: uuid.New(), TenantID: tenant, MemberID: uuid.New(), RoleID: uuid.New()}
	stranger := chat.Actor{UserID: uuid.New(), TenantID: tenant, MemberID: uuid.New()}
	svc := chat.Service{Store: store, Permission: permission(true)}
	const n = 12
	ids := make(chan uuid.UUID, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, _, err := svc.Create(ctx, owner)
			if err != nil {
				errs <- err
				return
			}
			ids <- c.ID
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var id uuid.UUID
	for got := range ids {
		if id != uuid.Nil && id != got {
			t.Fatal("duplicate conversations")
		}
		id = got
	}
	if id == uuid.Nil {
		t.Fatal("no conversation")
	}
	c, created, err := svc.Create(ctx, owner)
	if err != nil || created || c.ID != id {
		t.Fatalf("get-or-create: %+v %v %v", c, created, err)
	}
	m, err := svc.Send(ctx, owner, id, " hello ", "request-1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Body != "hello" {
		t.Fatal(m.Body)
	}
	again, err := svc.Send(ctx, owner, id, " changed ", "request-1")
	if err != nil || again.ID != m.ID || again.Body != m.Body {
		t.Fatalf("idempotency: %+v %v", again, err)
	}
	rows, err := svc.List(ctx, manager, 1, 20)
	if err != nil || len(rows) != 1 || rows[0].UnreadCount != 1 || rows[0].LastMessage == nil {
		t.Fatalf("manager list: %+v %v", rows, err)
	}
	crossTenant := chat.Actor{UserID: manager.UserID, TenantID: otherTenant, MemberID: manager.MemberID, RoleID: manager.RoleID}
	if rows, err := svc.List(ctx, crossTenant, 1, 20); err != nil || len(rows) != 0 {
		t.Fatalf("cross-tenant list: %+v %v", rows, err)
	}
	if _, err = svc.Get(ctx, crossTenant, id); err != chat.ErrNotFound {
		t.Fatalf("cross tenant: %v", err)
	}
	if _, err = (chat.Service{Store: store, Permission: permission(false)}).Get(ctx, stranger, id); err != chat.ErrNotFound {
		t.Fatalf("stranger: %v", err)
	}
	if _, err = svc.Get(ctx, chat.Actor{UserID: uuid.New(), IsParent: true}, id); err != chat.ErrNotFound {
		t.Fatalf("parent: %v", err)
	}
	if _, err = svc.Send(ctx, manager, id, "reply", "request-2"); err != nil {
		t.Fatal(err)
	}
	if err = svc.Read(ctx, manager, id); err != nil {
		t.Fatal(err)
	}
	rows, err = svc.List(ctx, manager, 1, 20)
	if err != nil || len(rows) != 1 || rows[0].UnreadCount != 0 {
		t.Fatalf("read: %+v %v", rows, err)
	}
}
