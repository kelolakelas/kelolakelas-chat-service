package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	chathttp "github.com/kelolakelas/kelolakelas-chat-service/internal/delivery/http"
)

type recoveryIdentity struct{ mode atomic.Int32 }

func (p *recoveryIdentity) CheckPermission(context.Context, string, string, string, string) (bool, error) {
	if p.mode.Load() == 1 {
		return false, errors.New("identity unavailable")
	}
	return p.mode.Load() == 0, nil
}
func (p *recoveryIdentity) CheckActiveMembership(context.Context, string, string) (bool, error) {
	return p.mode.Load() != 3, nil
}

func TestKEL223InboxPostgresRecovery(t *testing.T) {
	dsn := os.Getenv("KEL223_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set KEL223_TEST_DATABASE_URL to a disposable acceptance database")
	}
	ctx := context.Background()
	root, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	schema := "kel223_" + uuid.NewString()[:8]
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer root.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, name := range []string{"000001_chat.up.sql", "000002_context_chat.up.sql", "000003_notification.up.sql"} {
		raw, err := os.ReadFile(filepath.Join("../../migrations", name))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(ctx, string(raw)); err != nil {
			t.Fatal(err)
		}
	}
	tenant, parent := uuid.New(), uuid.New()
	actor := chat.Actor{UserID: uuid.New(), TenantID: tenant, RoleID: uuid.New(), MemberID: uuid.New()}
	identity := &recoveryIdentity{}
	store := Store{DB: db}
	svc := chat.Service{Store: store, Permission: identity, Membership: identity}
	var oldest uuid.UUID
	for i := 0; i < 105; i++ {
		c, _, err := store.Create(ctx, chat.Conversation{TenantID: tenant, Kind: "schedule_request", SubjectID: uuid.New(), ParentUserID: &parent, CreatedByUserID: parent, Context: json.RawMessage(fmt.Sprintf(`{"class_name":"Room %03d"}`, i))})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			oldest = c.ID
		}
		if _, err = db.Exec(ctx, "UPDATE conversations SET created_at=$2 WHERE id=$1", c.ID, time.Date(2026, 10, 1, 0, 0, i, 0, time.UTC)); err != nil {
			t.Fatal(err)
		}
	}
	h := chathttp.Handler{Service: svc, Secret: "kel223-test"}
	token := func(a chat.Actor) string {
		claims := jwt.MapClaims{"user_id": a.UserID.String(), "is_parent": a.IsParent, "exp": time.Now().Add(time.Hour).Unix()}
		if !a.IsParent {
			claims["tenant_id"] = a.TenantID.String()
			claims["role_id"] = a.RoleID.String()
			claims["member_id"] = a.MemberID.String()
		}
		value, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte("kel223-test"))
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	request := func(a chat.Actor, path string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/api/v1/chat/conversations"+path, nil)
		r.Header.Set("Authorization", "Bearer "+token(a))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	list := func(a chat.Actor, page int) []chat.Conversation {
		w := request(a, fmt.Sprintf("?page=%d&page_size=100", page))
		if w.Code != 200 {
			t.Fatalf("list %d %s", w.Code, w.Body)
		}
		var body struct {
			Data []chat.Conversation `json:"data"`
		}
		if err = json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		return body.Data
	}
	first, second := list(actor, 1), list(actor, 2)
	if len(first) != 100 || len(second) != 5 || second[4].ID != oldest {
		t.Fatalf("paging first=%d second=%d", len(first), len(second))
	}
	for _, c := range first {
		if c.ID == oldest {
			t.Fatal("oldest unexpectedly on first100")
		}
	}
	if w := request(actor, "/"+oldest.String()); w.Code != 200 {
		t.Fatalf("older room direct GET %d", w.Code)
	}
	var count int
	if err = db.QueryRow(ctx, "SELECT count(*) FROM conversations").Scan(&count); err != nil || count != 105 {
		t.Fatalf("persisted rows %d %v", count, err)
	}
	t.Logf("reproduction: persisted=%d, list page1=%d, page2=%d; oldest absent from page1 but direct GET=200", count, len(first), len(second))
	identity.mode.Store(1)
	for _, path := range []string{"?page=1&page_size=100", "/" + oldest.String(), "/" + oldest.String() + "/messages"} {
		w := request(actor, path)
		t.Logf("identity outage path=%s HTTP=%d", path, w.Code)
		if w.Code != 503 {
			t.Errorf("outage must be unavailable, not successful empty/not-found: %d %s", w.Code, w.Body)
		}
	}
	identity.mode.Store(0)
	if len(list(actor, 2)) != 5 {
		t.Fatal("outage recovery lost older rooms")
	}
	message, err := svc.Send(ctx, actor, oldest, "Reply to old room", "stable-client-id")
	if err != nil {
		t.Fatal(err)
	}
	again, err := svc.Send(ctx, actor, oldest, "Duplicate", "stable-client-id")
	if err != nil || again.ID != message.ID {
		t.Fatal("duplicate message")
	}
	if err = svc.Read(ctx, actor, oldest); err != nil {
		t.Fatal(err)
	}
	if list(actor, 1)[0].ID != oldest {
		t.Fatal("new message did not reorder older room")
	}
	identity.mode.Store(2)
	if len(list(actor, 1)) != 0 {
		t.Fatal("revoked permission retained rooms")
	}
	if w := request(actor, "/"+oldest.String()); w.Code != 404 {
		t.Fatalf("denied detail %d", w.Code)
	}
	// An active staff owner needs no manage permission, but a revoked membership
	// must not retain even this owner path.
	staff, _, err := store.Create(ctx, chat.Conversation{TenantID: tenant, Kind: "staff", SubjectID: actor.MemberID, MemberUserID: &actor.UserID, CreatedByUserID: actor.UserID, Context: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	if w := request(actor, "/"+staff.ID.String()); w.Code != 200 {
		t.Fatalf("active staff owner %d", w.Code)
	}
	identity.mode.Store(3)
	if w := request(actor, "?page=1&page_size=100"); w.Code != 403 {
		t.Errorf("revoked membership list %d", w.Code)
	}
	if w := request(actor, "/"+staff.ID.String()); w.Code != 404 {
		t.Errorf("revoked owner detail %d", w.Code)
	}
	identity.mode.Store(0)
	foreign := actor
	foreign.TenantID = uuid.New()
	if len(list(foreign, 1)) != 0 {
		t.Fatal("foreign tenant rooms leaked")
	}
	if w := request(foreign, "/"+oldest.String()); w.Code != 404 {
		t.Fatal("foreign detail leaked")
	}
	owner := chat.Actor{UserID: parent, IsParent: true}
	if len(list(owner, 1)) != 100 || len(list(owner, 2)) != 5 {
		t.Fatal("parent regression")
	}
	if len(list(chat.Actor{UserID: uuid.New(), IsParent: true}, 1)) != 0 {
		t.Fatal("wrong parent rooms leaked")
	}
}
