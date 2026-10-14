package postgres

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	"os"
	"testing"
	"time"
)

func TestPaymentLinkExpiryWhileWaitingForLock(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	store := Store{DB: db}
	parent := uuid.New()
	c := chat.Conversation{TenantID: uuid.New(), Kind: "schedule_request", SubjectID: uuid.New(), ParentUserID: &parent, CreatedByUserID: parent, Context: []byte(`{}`)}
	defer db.Exec(context.Background(), `DELETE FROM conversations WHERE tenant_id=$1`, c.TenantID)
	c, first, _, err := store.ScheduleRequestPaymentLink(ctx, c, "first", "first", time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"new", "first"} {
		t.Run(key, func(t *testing.T) {
			tx, err := db.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, `SELECT id FROM conversations WHERE id=$1 FOR UPDATE`, c.ID); err != nil {
				t.Fatal(err)
			}
			expiry := time.Now().Add(150 * time.Millisecond)
			result := make(chan error, 1)
			go func() {
				_, _, _, err := store.ScheduleRequestPaymentLink(ctx, c, "expired", key, expiry)
				result <- err
			}()
			// Confirm the write really waits on a database lock before letting expiry pass.
			for {
				var waiting bool
				if err := db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE 'INSERT INTO conversations%')`).Scan(&waiting); err != nil {
					t.Fatal(err)
				}
				if waiting {
					break
				}
				if time.Now().After(expiry) {
					t.Fatal("write did not reach lock before expiry")
				}
				time.Sleep(time.Millisecond)
			}
			time.Sleep(time.Until(expiry.Add(20 * time.Millisecond)))
			if err := tx.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-result; !errors.Is(err, chat.ErrPaymentLinkInactive) {
				t.Fatalf("expired write/retry: %v", err)
			}
			msgs, err := store.Messages(ctx, c.ID, nil, 100)
			if err != nil || len(msgs) != 1 || msgs[0].ID != first.ID {
				t.Fatalf("messages changed: %+v %v", msgs, err)
			}
		})
	}
}
