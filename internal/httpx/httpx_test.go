package httpx

import (
	"context"
	"net/http"
	"strings"
	"testing"
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
