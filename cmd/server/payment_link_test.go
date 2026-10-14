package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPaymentLinkRouteReachesHandler(t *testing.T) {
	called := false
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		if r.URL.Path != "/internal/schedule-request-payment-links" {
			t.Fatal(r.URL.Path)
		}
		w.WriteHeader(http.StatusUnauthorized)
	})
	w := httptest.NewRecorder()
	routes(h).ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/internal/schedule-request-payment-links", nil))
	if !called || w.Code != http.StatusUnauthorized {
		t.Fatalf("called=%v status=%d", called, w.Code)
	}
}
