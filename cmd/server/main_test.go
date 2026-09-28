package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

func TestHealth(t *testing.T) {
	rec := httptest.NewRecorder()
	newMux().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d", rec.Code)
	}
	if got, want := rec.Body.String(), `{"status":"healthy","service":"chat-service"}`; got != want {
		t.Fatalf("body=%s want %s", got, want)
	}
}

func TestHealthRejectsOtherMethods(t *testing.T) {
	rec := httptest.NewRecorder()
	newMux().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/health", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", rec.Code)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg := loadConfig(func(string) string { return "" })
	if cfg.Port != "8083" || cfg.ServerShutdownTimeout != 15*time.Second || cfg.ServerReadHeaderTimeout != 5*time.Second {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	env := map[string]string{"PORT": "9000", "SERVER_WRITE_TIMEOUT_SECONDS": "-1", "SERVER_IDLE_TIMEOUT_SECONDS": "7"}
	cfg = loadConfig(func(k string) string { return env[k] })
	if cfg.Port != "9000" || cfg.ServerWriteTimeout != 60*time.Second || cfg.ServerIdleTimeout != 7*time.Second {
		t.Fatalf("unexpected overrides: %+v", cfg)
	}
}

func TestLoadConfigDatabaseURL(t *testing.T) {
	secret := "p@ss:/?#% word"
	env := map[string]string{
		"DB_USER": "staff@tenant", "DB_PASSWORD": secret,
		"DB_HOST": "::1", "DB_PORT": "5543", "DB_NAME": "staff/db",
		"DB_SSLMODE": "require",
	}
	getenv := func(key string) string { return env[key] }
	cfg := loadConfig(getenv)
	parsed, err := url.Parse(cfg.DatabaseURL)
	if err != nil {
		t.Fatalf("parse fallback database URL: %v", err)
	}
	if parsed.Scheme != "postgres" || parsed.Host != "[::1]:5543" || parsed.Path != "/staff/db" || parsed.Query().Get("sslmode") != "require" {
		t.Fatalf("unexpected fallback URL components: scheme=%q host=%q path=%q sslmode=%q", parsed.Scheme, parsed.Host, parsed.Path, parsed.Query().Get("sslmode"))
	}
	username := parsed.User.Username()
	password, ok := parsed.User.Password()
	if username != env["DB_USER"] || !ok || password != secret {
		t.Fatal("fallback credentials did not round-trip")
	}
	if _, err := pgxpool.ParseConfig(cfg.DatabaseURL); err != nil {
		t.Fatal("fallback URL was rejected by pgx")
	}

	env["DATABASE_URL"] = "postgres://override:chosen@localhost/primary?sslmode=disable"
	cfg = loadConfig(getenv)
	if cfg.DatabaseURL != env["DATABASE_URL"] {
		t.Fatal("DATABASE_URL did not take precedence over DB_* settings")
	}
}

func TestServeUntilDoneStopsOnCancel(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- serveUntilDone(ctx, &http.Server{Handler: newMux(), ReadHeaderTimeout: time.Second}, listener, time.Second)
	}()
	resp, err := http.Get("http://" + listener.Addr().String() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveUntilDone: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not stop")
	}
}
