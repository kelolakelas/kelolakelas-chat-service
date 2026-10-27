package postgres

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func privateLifecycleDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("KEL226_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set KEL226_TEST_DATABASE_URL to a disposable database")
	}
	ctx := context.Background()
	root, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(root.Close)
	schema := "kel226_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE") })
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 12
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	files, _ := filepath.Glob("../../migrations/*.up.sql")
	sort.Strings(files)
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("migration %s %v", file, err)
		}
	}
	return db
}
func TestKEL226LifecyclePostgresAtomicityAndLockRecheck(t *testing.T) {
	db := privateLifecycleDB(t)
	ctx := context.Background()
	store := Store{DB: db}
	parent, admin := uuid.New(), uuid.New()
	c := chat.Conversation{TenantID: uuid.New(), Kind: "schedule_request", SubjectID: uuid.New(), ParentUserID: &parent, CreatedByUserID: parent, Context: []byte(`{"class_name":"Fixture"}`)}
	event := chat.PrivateLifecycleEvent{ID: uuid.New(), RequestID: c.SubjectID, TenantID: c.TenantID, ParentID: parent, ActorUserID: parent, ActorKind: "parent", Kind: "created", Body: "Parent requested two weekly slots"}
	refresh := func(context.Context) (chat.PrivateLifecycleEvent, error) { return event, nil }
	var wg sync.WaitGroup
	var created atomic.Int32
	errs := make(chan error, 8)
	ids := make(chan uuid.UUID, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			room, m, new, err := store.PrivateLifecycleMessage(ctx, c, event, refresh)
			if err == nil {
				if new {
					created.Add(1)
				}
				ids <- m.ID
				if room.SubjectID != c.SubjectID {
					err = errors.New("wrong room")
				}
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if created.Load() != 1 {
		t.Fatal("multiple first sends")
	}
	var first uuid.UUID
	for id := range ids {
		if first == uuid.Nil {
			first = id
		}
		if id != first {
			t.Fatal("divergent replay")
		}
	}
	room, _, _, err := store.PrivateLifecycleMessage(ctx, c, event, refresh)
	if err != nil {
		t.Fatal(err)
	}
	other := c
	other.SubjectID = uuid.New()
	if _, _, _, err := store.PrivateLifecycleMessage(ctx, other, event, refresh); !errors.Is(err, chat.ErrNotFound) {
		t.Fatalf("event leaked to another room: %v", err)
	}
	// Approval initially appears payable. Block the room, then settle it before
	// releasing the lock: the under-lock callback must suppress the stale URL.
	approval := event
	approval.ID = uuid.New()
	approval.Kind = "approved"
	approval.ActorKind = "member"
	approval.ActorUserID = admin
	expiry := time.Now().Add(time.Hour)
	approval.InvoiceExpiresAt = &expiry
	approval.Body = "Pay: https://pay.example.test"
	locker, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = locker.Exec(ctx, "SELECT id FROM conversations WHERE id=$1 FOR UPDATE", room.ID); err != nil {
		t.Fatal(err)
	}
	var paid atomic.Bool
	var calls atomic.Int32
	out := make(chan chat.Message, 1)
	result := make(chan error, 1)
	go func() {
		_, m, _, err := store.PrivateLifecycleMessage(ctx, c, approval, func(context.Context) (chat.PrivateLifecycleEvent, error) {
			calls.Add(1)
			fresh := approval
			if paid.Load() {
				fresh.Body = "Payment already received"
				fresh.InvoiceExpiresAt = nil
			}
			return fresh, nil
		})
		out <- m
		result <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatal("invoice rechecked before lock")
	}
	paid.Store(true)
	if err = locker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-result; err != nil {
		t.Fatal(err)
	}
	m := <-out
	if strings.Contains(m.Body, "https://") || m.SenderKind != "member" || *m.SenderUserID != admin {
		t.Fatal("stale link or actor")
	}
	// An unavailable recheck rolls back the new room and owed message; retry
	// commits once, including after worker restart with the same event key.
	freshRoom := c
	freshRoom.SubjectID = uuid.New()
	rejection := event
	rejection.ID = uuid.New()
	rejection.RequestID = freshRoom.SubjectID
	rejection.Kind = "rejected"
	rejection.ActorKind = "member"
	rejection.ActorUserID = admin
	rejection.Body = "Rejected with recommendation"
	if _, _, _, err = store.PrivateLifecycleMessage(ctx, freshRoom, rejection, func(context.Context) (chat.PrivateLifecycleEvent, error) { return rejection, chat.ErrUnavailable }); !errors.Is(err, chat.ErrUnavailable) {
		t.Fatal(err)
	}
	var count int
	db.QueryRow(ctx, "SELECT count(*) FROM conversations WHERE subject_id=$1", freshRoom.SubjectID).Scan(&count)
	if count != 0 {
		t.Fatal("rollback kept room")
	}
	if _, _, new, err := store.PrivateLifecycleMessage(ctx, freshRoom, rejection, func(context.Context) (chat.PrivateLifecycleEvent, error) { return rejection, nil }); err != nil || !new {
		t.Fatalf("recovery %v", err)
	}
	expired := approval
	expired.ID = uuid.New()
	expiredTime := time.Now().Add(-time.Second)
	expired.InvoiceExpiresAt = &expiredTime
	if _, _, _, err := store.PrivateLifecycleMessage(ctx, c, expired, func(context.Context) (chat.PrivateLifecycleEvent, error) { return expired, nil }); !errors.Is(err, chat.ErrPaymentLinkInactive) {
		t.Fatalf("expired write: %v", err)
	}
	long := rejection
	long.ID = uuid.New()
	long.Body = strings.Repeat("jadwal ", 600)
	if _, _, _, err := store.PrivateLifecycleMessage(ctx, freshRoom, long, func(context.Context) (chat.PrivateLifecycleEvent, error) { return long, nil }); err != nil {
		t.Fatalf("complete lifecycle body rejected: %v", err)
	}
	// Parent unread excludes their own authored event and counts admin events.
	rows, err := store.ListOwner(ctx, chat.Actor{UserID: parent, IsParent: true}, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	var unread int
	for _, row := range rows {
		if row.ID == room.ID {
			unread = row.UnreadCount
		}
	}
	if unread != 1 {
		t.Fatalf("unread %d", unread)
	}
}
