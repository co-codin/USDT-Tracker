// Package server exposes /metrics and /healthz, plus any extra handlers
// mounted with Handle (e.g. the admin API under /v1/).
package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

// HealthFunc reports health and a JSON-serialisable detail payload.
type HealthFunc func(ctx context.Context) (healthy bool, detail any)

// Server is a small HTTP server for operational endpoints.
type Server struct {
	srv  *http.Server
	mux  *http.ServeMux
	log  *slog.Logger
	mu   sync.Mutex
	addr net.Addr
}

// New builds the server. metrics may be nil.
func New(addr string, metrics http.Handler, health HealthFunc, log *slog.Logger) *Server {
	mux := http.NewServeMux()
	if metrics != nil {
		mux.Handle("GET /metrics", metrics)
	}
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		ok, detail := health(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if !ok {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": ok, "status": detail})
	})
	return &Server{
		srv: &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second},
		mux: mux,
		log: log,
	}
}

// Handle mounts h on pattern (http.ServeMux syntax). Call before Start.
func (s *Server) Handle(pattern string, h http.Handler) { s.mux.Handle(pattern, h) }

// Start listens in the background. It returns an error if the port is taken.
func (s *Server) Start() error {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.srv.Addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.addr = ln.Addr()
	s.mu.Unlock()
	s.log.Info("http server listening", "addr", ln.Addr().String())
	go func() {
		if err := s.srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.log.Error("http server failed", "err", err)
		}
	}()
	return nil
}

// Addr returns the bound address (useful with ":0"); nil before Start.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.addr
}

// Shutdown stops the server gracefully.
func (s *Server) Shutdown(ctx context.Context) error { return s.srv.Shutdown(ctx) }
