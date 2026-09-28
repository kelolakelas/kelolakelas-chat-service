package postgres

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	"github.com/kelolakelas/kelolakelas-chat-service/pkg/academic"
)

type contextPermission struct{}

func (contextPermission) CheckPermission(_ context.Context, _, _, _, name string) (bool, error) {
	return name == "chat:manage" || name == "report:read", nil
}

type contextTenant struct{}

func (contextTenant) Name(context.Context, uuid.UUID) (string, error) { return "School", nil }
func TestPostgresAcademicIsolation(t *testing.T) {
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
	tenant, parent, subject := uuid.New(), uuid.New(), uuid.New()
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Service-Credential") != "credential" {
			w.WriteHeader(401)
			return
		}
		fmt.Fprintf(w, `{"data":{"id":%q,"tenant_id":%q,"parent_id":%q,"class_name":"Private","student_first_name":"Kid","title":"Weekly"}}`, subject, tenant, parent)
	}))
	defer fake.Close()
	academicClient, err := academic.New(fake.URL, "credential", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	svc := chat.Service{Store: Store{DB: db}, Permission: contextPermission{}, Academic: academicClient, Tenant: contextTenant{}}
	owner := chat.Actor{UserID: parent, IsParent: true}
	other := chat.Actor{UserID: uuid.New(), IsParent: true}
	manager := chat.Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	wrong := manager
	wrong.TenantID = uuid.New()
	for _, kind := range []string{"schedule_request", "report"} {
		actor := manager
		if kind == "schedule_request" {
			actor = owner
		}
		const workers = 4
		ids := make(chan uuid.UUID, workers)
		errs := make(chan error, workers)
		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				c, _, err := svc.Create(ctx, actor, chat.CreateInput{Kind: kind, SubjectID: subject})
				if err != nil {
					errs <- err
				} else {
					ids <- c.ID
				}
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
				t.Fatal("duplicate")
			}
			id = got
		}
		if id == uuid.Nil {
			t.Fatal("missing conversation")
		}
		for _, denied := range []chat.Actor{other, wrong} {
			if _, err := svc.Get(ctx, denied, id); err != chat.ErrNotFound {
				t.Fatalf("%s unauthorized get %v", kind, err)
			}
			if _, err := svc.Send(ctx, denied, id, "hi", "denied"); err != chat.ErrNotFound {
				t.Fatalf("%s unauthorized send %v", kind, err)
			}
		}
		if rows, err := svc.List(ctx, other, 1, 20); err != nil || len(rows) != 0 {
			t.Fatalf("other list %+v %v", rows, err)
		}
		if rows, err := svc.List(ctx, wrong, 1, 20); err != nil || len(rows) != 0 {
			t.Fatalf("wrong tenant list %+v %v", rows, err)
		}
		if _, err := svc.Send(ctx, owner, id, "hello", "parent"); err != nil {
			t.Fatal(err)
		}
		if _, err := svc.Send(ctx, manager, id, "reply", "member"); err != nil {
			t.Fatal(err)
		}
		rows, err := svc.Messages(ctx, owner, id, nil, 10)
		if err != nil || len(rows) != 2 || rows[1].SenderKind != "parent" {
			t.Fatalf("messages %+v %v", rows, err)
		}
		if rows, err := svc.List(ctx, owner, 1, 20); err != nil || len(rows) == 0 {
			t.Fatalf("parent list %+v %v", rows, err)
		}
	}
}
