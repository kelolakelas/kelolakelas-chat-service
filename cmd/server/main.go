// Command server runs the KelolaKelas chat service.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
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
}

func loadConfig(getenv func(string) string) config {
	port := getenv("PORT")
	if port == "" {
		port = "8083"
	}
	return config{
		Port:                    port,
		ServerReadHeaderTimeout: seconds(getenv("SERVER_READ_HEADER_TIMEOUT_SECONDS"), 5),
		ServerReadTimeout:       seconds(getenv("SERVER_READ_TIMEOUT_SECONDS"), 30),
		ServerWriteTimeout:      seconds(getenv("SERVER_WRITE_TIMEOUT_SECONDS"), 60),
		ServerIdleTimeout:       seconds(getenv("SERVER_IDLE_TIMEOUT_SECONDS"), 120),
		ServerShutdownTimeout:   seconds(getenv("SERVER_SHUTDOWN_TIMEOUT_SECONDS"), 15),
	}
}

// seconds parses a positive whole number of seconds; anything else uses def.
func seconds(raw string, def int) time.Duration {
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		n = def
	}
	return time.Duration(n) * time.Second
}

func newMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	server := newHTTPServer(cfg, newMux())
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
