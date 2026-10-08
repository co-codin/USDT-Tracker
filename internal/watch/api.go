package watch

// Admin HTTP API for the runtime watch list:
//
//	GET    /v1/addresses            list runtime (and static) watch addresses
//	POST   /v1/addresses            add {address, label?, direction?}
//	DELETE /v1/addresses/{address}  remove a runtime address
//
// Every request needs "Authorization: Bearer <API_TOKEN>". With an empty
// token the API is disabled (404 for every /v1 path); without a store
// (Postgres sink disabled) authenticated requests get 503.

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

// MaxBodyBytes caps request bodies.
const MaxBodyBytes = 4 << 10

// APIConfig configures the admin API handler.
type APIConfig struct {
	Token  string   // bearer token; empty disables the API
	Store  Store    // nil = Postgres disabled (503)
	Static []string // static config addresses, reported read-only by GET
	// OnChange is called after a successful add/delete (e.g. to invalidate
	// the filter's cached watch list). May be nil.
	OnChange func()
	Log      *slog.Logger
}

// API serves /v1/*. Mount it with mux.Handle("/v1/", h).
type API struct {
	cfg       APIConfig
	tokenHash [sha256.Size]byte
	mux       *http.ServeMux
}

// NewAPI builds the admin API handler.
func NewAPI(cfg APIConfig) *API {
	if cfg.Log == nil {
		cfg.Log = slog.New(slog.DiscardHandler)
	}
	if cfg.OnChange == nil {
		cfg.OnChange = func() {}
	}
	h := &API{cfg: cfg, tokenHash: sha256.Sum256([]byte(cfg.Token)), mux: http.NewServeMux()}
	h.mux.HandleFunc("GET /v1/addresses", h.list)
	h.mux.HandleFunc("POST /v1/addresses", h.add)
	h.mux.HandleFunc("DELETE /v1/addresses/{address}", h.remove)
	h.mux.HandleFunc("/v1/addresses", methodNotAllowed("GET, POST"))
	h.mux.HandleFunc("/v1/addresses/{address}", methodNotAllowed("DELETE"))
	h.mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		writeError(w, http.StatusNotFound, "not found")
	})
	return h
}

// Enabled reports whether a token is configured.
func (h *API) Enabled() bool { return h.cfg.Token != "" }

// ServeHTTP implements http.Handler: disabled → 404, bad token → 401,
// no store → 503, then routing.
func (h *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !h.Enabled() {
		writeError(w, http.StatusNotFound, "admin API disabled: set API_TOKEN (or api.token) to enable it")
		return
	}
	if !h.authorized(r) {
		w.Header().Set("WWW-Authenticate", `Bearer realm="usdt-tracker"`)
		writeError(w, http.StatusUnauthorized, "missing or invalid bearer token")
		return
	}
	if h.cfg.Store == nil {
		writeError(w, http.StatusServiceUnavailable,
			"runtime watch list requires the postgres sink (set SINK_POSTGRES_ENABLED=true and DATABASE_URL); static filter.watch_addresses still apply")
		return
	}
	h.mux.ServeHTTP(w, r)
}

func (h *API) authorized(r *http.Request) bool {
	scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return false
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return false
	}
	got := sha256.Sum256([]byte(token)) // fixed-length compare: no length leak
	return subtle.ConstantTimeCompare(got[:], h.tokenHash[:]) == 1
}

type listResponse struct {
	Addresses []Entry  `json:"addresses"`
	Static    []string `json:"static_addresses"`
}

func (h *API) list(w http.ResponseWriter, r *http.Request) {
	rows, err := h.cfg.Store.ListWatches(r.Context())
	if err != nil {
		h.internal(w, "list", err)
		return
	}
	if rows == nil {
		rows = []Entry{}
	}
	static := h.cfg.Static
	if static == nil {
		static = []string{}
	}
	writeJSON(w, http.StatusOK, listResponse{Addresses: rows, Static: static})
}

type addRequest struct {
	Address   string  `json:"address"`
	Label     *string `json:"label"`
	Direction string  `json:"direction"`
}

func (h *API) add(w http.ResponseWriter, r *http.Request) {
	var req addRequest
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if dec.Decode(&struct{}{}) != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid JSON body: trailing data")
		return
	}
	if req.Address == "" {
		writeError(w, http.StatusBadRequest, "address is required")
		return
	}
	addr, err := NormalizeAddress(req.Address)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("address %q: %v", req.Address, err))
		return
	}
	dir, err := NormalizeDirection(req.Direction)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	label, err := NormalizeLabel(req.Label)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	e, err := h.cfg.Store.AddWatch(r.Context(), Entry{Address: addr, Label: label, Direction: dir, Enabled: true})
	if errors.Is(err, ErrExists) {
		writeError(w, http.StatusConflict, "address already watched: "+addr)
		return
	}
	if err != nil {
		h.internal(w, "add", err)
		return
	}
	h.cfg.OnChange()
	h.cfg.Log.Info("watch address added", "address", addr, "direction", dir)
	writeJSON(w, http.StatusCreated, e)
}

func (h *API) remove(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("address")
	addr, err := NormalizeAddress(raw)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("address %q: %v", raw, err))
		return
	}
	err = h.cfg.Store.DeleteWatch(r.Context(), addr)
	if errors.Is(err, ErrNotFound) {
		writeError(w, http.StatusNotFound, "address not in the runtime watch list: "+addr)
		return
	}
	if err != nil {
		h.internal(w, "delete", err)
		return
	}
	h.cfg.OnChange()
	h.cfg.Log.Info("watch address removed", "address", addr)
	w.WriteHeader(http.StatusNoContent)
}

func methodNotAllowed(allow string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Allow", allow)
		writeError(w, http.StatusMethodNotAllowed, "method not allowed; use "+allow)
	}
}

func (h *API) internal(w http.ResponseWriter, op string, err error) {
	h.cfg.Log.Error("admin api: storage error", "op", op, "err", err)
	writeError(w, http.StatusInternalServerError, "storage error")
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
