package httpx

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedactURL(t *testing.T) {
	for in, want := range map[string]string{
		"https://h.example/hook?token=SECRET": "https://h.example/***",
		"https://u:p@h.example":               "https://h.example/***",
		"https://h.example/":                  "https://h.example",
		"::bad":                               "<invalid url>",
	} {
		if got := RedactURL(in); got != want {
			t.Errorf("RedactURL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestScrubURLError(t *testing.T) {
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://127.0.0.1:1/x?token=SECRET123", nil)
	_, err := http.DefaultClient.Do(req)
	if err == nil || !strings.Contains(err.Error(), "SECRET123") {
		t.Fatalf("precondition: %v", err)
	}
	if s := ScrubURLError(err).Error(); strings.Contains(s, "SECRET123") {
		t.Fatalf("still leaks: %s", s)
	}
}

func TestRedirectsStayOnHost(t *testing.T) {
	var leaked atomic.Bool
	other := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-API-Key") != "" {
			leaked.Store(true)
		}
	}))
	defer other.Close()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/same":
			http.Redirect(w, r, srv.URL+"/ok", http.StatusFound)
		case "/ok":
			_, _ = io.WriteString(w, "fine")
		default:
			http.Redirect(w, r, other.URL+"/steal", http.StatusFound)
		}
	}))
	defer srv.Close()
	hc := NewClient(time.Second)
	get := func(p string) (*http.Response, error) {
		req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL+p, nil)
		req.Header.Set("X-API-Key", "SECRET")
		return hc.Do(req)
	}
	resp, err := get("/same")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := ReadBody(resp, 2)
	if string(b) != "fi" {
		t.Fatalf("body %q", b)
	}
	if _, err := get("/cross"); err == nil || leaked.Load() {
		t.Fatalf("cross-host redirect followed (err %v, leaked %v)", err, leaked.Load())
	}
}
