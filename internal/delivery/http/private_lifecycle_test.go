package chathttp

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestPrivateLifecycleCredentialAndSenderInjection(t *testing.T) {
	h := Handler{InternalCredential: "internal-fixture"}
	for _, tc := range []struct {
		method, credential, body string
		status                   int
	}{{"GET", "", "", 405}, {"POST", "", `{}`, 401}, {"POST", "wrong", `{}`, 401}, {"POST", "internal-fixture", `{"event_id":"00000000-0000-0000-0000-000000000001","sender_user_id":"forged"}`, 400}} {
		r := httptest.NewRequest(tc.method, "/internal/private-request-events", strings.NewReader(tc.body))
		r.Header.Set("X-Internal-Service-Credential", tc.credential)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("got%d want%d", w.Code, tc.status)
		}
	}
}
