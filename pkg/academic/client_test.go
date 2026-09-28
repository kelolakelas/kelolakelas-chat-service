package academic

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
)

func TestContextHTTP(t *testing.T) {
	id := uuid.New()
	status := 200
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Internal-Service-Credential") != "test-credential" {
			t.Error("credential absent")
		}
		if !strings.HasSuffix(r.URL.Path, id.String()) {
			t.Error("wrong subject")
		}
		w.WriteHeader(status)
		if status == 200 {
			fmt.Fprintf(w, `{"data":{"id":%q,"tenant_id":%q,"parent_id":%q,"class_name":"Math","title":"Weekly"}}`, id, id, id)
		} else {
			fmt.Fprint(w, "private academic database error")
		}
	}))
	defer server.Close()
	c, err := New(server.URL, "test-credential", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	item, err := c.Context(context.Background(), "report", id)
	if err != nil || item.ID != id || item.Title != "Weekly" {
		t.Fatalf("%+v %v", item, err)
	}
	status = 404
	if _, err = c.Context(context.Background(), "schedule_request", id); !errors.Is(err, chat.ErrNotFound) {
		t.Fatal(err)
	}
	status = 500
	if _, err = c.Context(context.Background(), "report", id); !errors.Is(err, chat.ErrUnavailable) || strings.Contains(err.Error(), "private") {
		t.Fatal(err)
	}
	server.Close()
	if _, err = c.Context(context.Background(), "report", id); !errors.Is(err, chat.ErrUnavailable) {
		t.Fatal(err)
	}
}
