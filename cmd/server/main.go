// Command server runs the KelolaKelas chat service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/chat"
	chathttp "github.com/kelolakelas/kelolakelas-chat-service/internal/delivery/http"
	"github.com/kelolakelas/kelolakelas-chat-service/internal/postgres"
	"github.com/kelolakelas/kelolakelas-chat-service/pkg/grpcclient"
)

const serviceName = "chat-service"

// config holds the settings the skeleton needs; each later issue extends it.
type config struct {
	Port                    string
	ServerReadHeaderTimeout time.Duration
	ServerReadTimeout       time.Duration
	ServerWriteTimeout      time.Duration
	ServerIdleTimeout       time.Duration
	ServerShutdownTimeout   time.Duration
	DatabaseURL             string
	JWTSecret               string
	IdentityHost            string
	PermissionTimeout       time.Duration
}

func loadConfig(getenv func(string) string) config {
	port := getenv("PORT")
	if port == "" {
		port = "8083"
	}
	host := strings.TrimSpace(getenv("IDENTITY_GRPC_HOST"))
	if host == "" {
		host = "localhost:50051"
	}
	db := strings.TrimSpace(getenv("DATABASE_URL"))
	if db == "" {
		endpoint := url.URL{
			Scheme: "postgres",
			User:   url.UserPassword(fallback(getenv("DB_USER"), "postgres"), getenv("DB_PASSWORD")),
			Host:   net.JoinHostPort(fallback(getenv("DB_HOST"), "localhost"), fallback(getenv("DB_PORT"), "5432")),
			Path:   "/" + fallback(getenv("DB_NAME"), "chat"),
		}
		query := url.Values{"sslmode": {fallback(getenv("DB_SSLMODE"), "disable")}}
		endpoint.RawQuery = query.Encode()
		db = endpoint.String()
	}
	return config{
		DatabaseURL: db, JWTSecret: getenv("JWT_SECRET"), IdentityHost: host, PermissionTimeout: milliseconds(getenv("IDENTITY_PERMISSION_TIMEOUT_MS"), 500),
		Port:                    port,
		ServerReadHeaderTimeout: seconds(getenv("SERVER_READ_HEADER_TIMEOUT_SECONDS"), 5),
		ServerReadTimeout:       seconds(getenv("SERVER_READ_TIMEOUT_SECONDS"), 30),
		ServerWriteTimeout:      seconds(getenv("SERVER_WRITE_TIMEOUT_SECONDS"), 60),
		ServerIdleTimeout:       seconds(getenv("SERVER_IDLE_TIMEOUT_SECONDS"), 120),
		ServerShutdownTimeout:   seconds(getenv("SERVER_SHUTDOWN_TIMEOUT_SECONDS"), 15),
	}
}

func fallback(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
func milliseconds(raw string, def int) time.Duration {
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		n = def
	}
	return time.Duration(n) * time.Millisecond
}

// seconds parses a positive whole number of seconds; anything else uses def.
func seconds(raw string, def int) time.Duration {
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		n = def
	}
	return time.Duration(n) * time.Second
}

func newMux() *http.ServeMux { return routes(nil) }
func routes(handler http.Handler) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	if handler != nil {
		mux.Handle("/api/v1/chat/conversations", handler)
		mux.Handle("/api/v1/chat/conversations/", handler)
	}
	return mux
}

// healthHandler is the liveness probe: 200 while the process serves HTTP.
func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"status":"healthy","service":%q}`, serviceName)
}

func newHTTPServer(cfg config, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              "0.0.0.0:" + cfg.Port,
		Handler:           handler,
		ReadHeaderTimeout: cfg.ServerReadHeaderTimeout,
		ReadTimeout:       cfg.ServerReadTimeout,
		WriteTimeout:      cfg.ServerWriteTimeout,
		IdleTimeout:       cfg.ServerIdleTimeout,
	}
}

// serveUntilDone serves until ctx is cancelled, then drains in-flight requests for at
// most shutdownTimeout before closing the remaining connections.
func serveUntilDone(ctx context.Context, server *http.Server, listener net.Listener, shutdownTimeout time.Duration) error {
	serveErr := make(chan error, 1)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- fmt.Errorf("serve HTTP: %w", err)
			return
		}
		serveErr <- nil
	}()
	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	var shutdownErr error
	if err := server.Shutdown(shutdownCtx); err != nil {
		shutdownErr = errors.Join(fmt.Errorf("HTTP shutdown did not finish in time: %w", err), server.Close())
	}
	return errors.Join(shutdownErr, <-serveErr)
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg := loadConfig(os.Getenv)
	if cfg.JWTSecret == "" {
		slog.Error("JWT_SECRET is required")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("database configuration invalid")
		os.Exit(1)
	}
	defer db.Close()
	if err = db.Ping(ctx); err != nil {
		slog.Error("database unavailable")
		os.Exit(1)
	}
	permission, err := grpcclient.New(cfg.IdentityHost, cfg.PermissionTimeout)
	if err != nil {
		slog.Error("identity client unavailable", "error", err)
		os.Exit(1)
	}
	defer permission.Close()
	handler := chathttp.Handler{Service: chat.Service{Store: postgres.Store{DB: db}, Permission: permission}, Secret: cfg.JWTSecret}
	server := newHTTPServer(cfg, routes(handler))
	listener, err := net.Listen("tcp", server.Addr)
	if err != nil {
		slog.Error("listen failed", "error", err)
		os.Exit(1)
	}
	slog.Info("Chat service listening", "addr", listener.Addr().String())
	if err := serveUntilDone(ctx, server, listener, cfg.ServerShutdownTimeout); err != nil {
		slog.Error("server stopped with error", "error", err)
		os.Exit(1)
	}
	slog.Info("Chat service stopped gracefully")
}
