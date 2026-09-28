package chathttp

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	"github.com/kelolakelas/kelolakelas-chat-service/pkg/academic"
)

type nameStub struct{}

func (nameStub) Name(context.Context, uuid.UUID) (string, error) { return "School", nil }
func TestCreateAcademicUnavailableSanitized(t *testing.T) {
	id, parent := uuid.New(), uuid.New()
	status := 500
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		fmt.Fprint(w, "private-db-password")
	}))
	defer upstream.Close()
	client, err := academic.New(upstream.URL, "internal-key", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	h := Handler{Secret: "secret", Service: chat.Service{Store: stubStore{}, Academic: client, Tenant: nameStub{}}}
	for _, state := range []int{500, 503, 404} {
		status = state
		req := httptest.NewRequest("POST", "/api/v1/chat/conversations", strings.NewReader(fmt.Sprintf(`{"kind":"schedule_request","subject_id":%q}`, id)))
		req.Header.Set("Authorization", signed(parent.String(), "", "", "", true))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		want := 503
		if state == 404 {
			want = 404
		}
		if rec.Code != want || strings.Contains(rec.Body.String(), "private-db-password") {
			t.Fatalf("status %d got %d %s", state, rec.Code, rec.Body.String())
		}
	}
}
