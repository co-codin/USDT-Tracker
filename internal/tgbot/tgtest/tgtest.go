// Package tgtest is an in-process mock of the Telegram Bot API (sendMessage
// and getUpdates) for tests, with fault injection (429 retry_after, 403).
package tgtest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"time"
)

// Sent is a recorded sendMessage call.
type Sent struct {
	ChatID    string
	Text      string
	ParseMode string
	At        time.Time
}

// Server is a mock Bot API for one token.
type Server struct {
	*httptest.Server
	Token string

	mu       sync.Mutex
	sent     []Sent
	updates  []map[string]any
	nextID   int64
	fail429  int
	forbid   map[string]bool
	polls    int
	badToken int
}

// New starts a mock for token.
func New(token string) *Server {
	s := &Server{Token: token, forbid: map[string]bool{}, nextID: 1}
	s.Server = httptest.NewServer(http.HandlerFunc(s.serve))
	return s
}

// Fail429 makes the next k sendMessage calls fail with 429 retry_after=1.
func (s *Server) Fail429(k int) { s.mu.Lock(); s.fail429 = k; s.mu.Unlock() }

// Forbid makes sendMessage to chatID fail with 403 (bot blocked).
func (s *Server) Forbid(chatID string) { s.mu.Lock(); s.forbid[chatID] = true; s.mu.Unlock() }

// Sent returns all successful sendMessage calls.
func (s *Server) Sent() []Sent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Sent(nil), s.sent...)
}

// SentTo returns successful messages for chatID.
func (s *Server) SentTo(chatID string) []Sent {
	var out []Sent
	for _, m := range s.Sent() {
		if m.ChatID == chatID {
			out = append(out, m)
		}
	}
	return out
}

// BadTokenCalls counts requests with a wrong token.
func (s *Server) BadTokenCalls() int { s.mu.Lock(); defer s.mu.Unlock(); return s.badToken }

// UserSays queues a private text message from user/chat id.
func (s *Server) UserSays(chatID int64, text string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, map[string]any{
		"update_id": s.nextID,
		"message": map[string]any{
			"message_id": s.nextID, "text": text, "date": time.Now().Unix(),
			"chat": map[string]any{"id": chatID, "type": "private"},
			"from": map[string]any{"id": chatID, "is_bot": false, "username": "u"},
		},
	})
	s.nextID++
}

// PendingUpdates returns updates not yet confirmed via offset.
func (s *Server) PendingUpdates() int { s.mu.Lock(); defer s.mu.Unlock(); return len(s.updates) }

func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	prefix := "/bot" + s.Token + "/"
	if !strings.HasPrefix(r.URL.Path, prefix) {
		s.mu.Lock()
		s.badToken++
		s.mu.Unlock()
		writeJSON(w, http.StatusUnauthorized, map[string]any{"ok": false, "error_code": 401, "description": "Unauthorized"})
		return
	}
	var p map[string]any
	_ = json.NewDecoder(r.Body).Decode(&p)
	switch strings.TrimPrefix(r.URL.Path, prefix) {
	case "sendMessage":
		chat := toString(p["chat_id"])
		s.mu.Lock()
		if s.fail429 > 0 {
			s.fail429--
			s.mu.Unlock()
			writeJSON(w, http.StatusTooManyRequests, map[string]any{"ok": false, "error_code": 429,
				"description": "Too Many Requests: retry after 1", "parameters": map[string]any{"retry_after": 1}})
			return
		}
		if s.forbid[chat] {
			s.mu.Unlock()
			writeJSON(w, http.StatusForbidden, map[string]any{"ok": false, "error_code": 403, "description": "Forbidden: bot was blocked by the user"})
			return
		}
		text, _ := p["text"].(string)
		if text == "" || len([]rune(text)) > 4096 {
			s.mu.Unlock()
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error_code": 400, "description": "Bad Request: message text is empty or too long"})
			return
		}
		pm, _ := p["parse_mode"].(string)
		s.sent = append(s.sent, Sent{ChatID: chat, Text: text, ParseMode: pm, At: time.Now()})
		s.mu.Unlock()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": map[string]any{"message_id": 1}})
	case "getUpdates":
		offset := int64(toFloat(p["offset"]))
		timeout := time.Duration(toFloat(p["timeout"])) * time.Second
		deadline := time.Now().Add(min(timeout, 200*time.Millisecond)) // keep tests fast
		for {
			s.mu.Lock()
			s.polls++
			kept := s.updates[:0]
			for _, u := range s.updates {
				if int64(toFloat(u["update_id"])) >= offset {
					kept = append(kept, u)
				}
			}
			s.updates = kept
			out := append([]map[string]any(nil), s.updates...)
			s.mu.Unlock()
			if len(out) > 0 || time.Now().After(deadline) || r.Context().Err() != nil {
				writeJSON(w, http.StatusOK, map[string]any{"ok": true, "result": out})
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"ok": false, "error_code": 404, "description": "Not Found"})
	}
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case float64:
		return strings.TrimSuffix(strings.TrimSuffix(jsonNum(x), ".0"), ".")
	default:
		return ""
	}
}

func jsonNum(f float64) string { b, _ := json.Marshal(f); return string(b) }

func toFloat(v any) float64 {
	f, _ := v.(float64)
	return f
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
