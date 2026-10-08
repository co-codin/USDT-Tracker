package watch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	token = "test-token-0123456789abcdef"
	alice = "TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yx"
	bob   = "TLaGjwhvA8XQYSxFAcAXy7Dvuue9eGYitv"
)

type client struct {
	t   *testing.T
	srv *httptest.Server
}

func newClient(t *testing.T, cfg APIConfig) *client {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("/v1/", NewAPI(cfg))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &client{t: t, srv: srv}
}

// do sends a request; auth "" omits the Authorization header.
func (c *client) do(method, path, auth, body string) (int, string, http.Header) {
	c.t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, c.srv.URL+path, r)
	if err != nil {
		c.t.Fatal(err)
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.srv.Client().Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), resp.Header
}

const bearer = "Bearer " + token

func TestDisabledWithoutToken(t *testing.T) {
	c := newClient(t, APIConfig{Store: NewMemoryStore()})
	for _, auth := range []string{"", "Bearer ", "Bearer anything"} {
		code, body, _ := c.do("GET", "/v1/addresses", auth, "")
		if code != http.StatusNotFound || !strings.Contains(body, "API_TOKEN") {
			t.Fatalf("auth %q: %d %s", auth, code, body)
		}
	}
	code, _, _ := c.do("POST", "/v1/addresses", "Bearer ", `{"address":"`+alice+`"}`)
	if code != http.StatusNotFound {
		t.Fatalf("POST on disabled API: %d", code)
	}
}

func TestAuth(t *testing.T) {
	store := NewMemoryStore()
	c := newClient(t, APIConfig{Token: token, Store: store})
	for _, auth := range []string{
		"",                               // missing
		token,                            // no scheme
		"Basic " + token,                 // wrong scheme
		"Bearer",                         // no token
		"Bearer " + token + "x",          // wrong
		"Bearer " + token[:len(token)-1], // prefix
		"Bearer wrong",
	} {
		code, body, hdr := c.do("GET", "/v1/addresses", auth, "")
		if code != http.StatusUnauthorized || hdr.Get("WWW-Authenticate") == "" || !strings.Contains(body, "error") {
			t.Errorf("auth %q: %d %s", auth, code, body)
		}
		if code, _, _ := c.do("POST", "/v1/addresses", auth, `{"address":"`+alice+`"}`); code != http.StatusUnauthorized {
			t.Errorf("POST auth %q: %d", auth, code)
		}
		if code, _, _ := c.do("DELETE", "/v1/addresses/"+alice, auth, ""); code != http.StatusUnauthorized {
			t.Errorf("DELETE auth %q: %d", auth, code)
		}
	}
	if all, _ := store.ListWatches(context.Background()); len(all) != 0 {
		t.Fatalf("unauthenticated requests changed the store: %+v", all)
	}
	for _, auth := range []string{bearer, "bearer " + token, "BEARER  " + token} {
		if code, body, _ := c.do("GET", "/v1/addresses", auth, ""); code != http.StatusOK {
			t.Errorf("auth %q: %d %s", auth, code, body)
		}
	}
}

func TestNoStoreReturns503(t *testing.T) {
	c := newClient(t, APIConfig{Token: token})
	for _, r := range [][3]string{
		{"GET", "/v1/addresses", ""},
		{"POST", "/v1/addresses", `{"address":"` + alice + `"}`},
		{"DELETE", "/v1/addresses/" + alice, ""},
	} {
		code, body, _ := c.do(r[0], r[1], bearer, r[2])
		if code != http.StatusServiceUnavailable || !strings.Contains(body, "postgres") {
			t.Errorf("%s %s: %d %s", r[0], r[1], code, body)
		}
	}
	// Auth is still checked first.
	if code, _, _ := c.do("GET", "/v1/addresses", "", ""); code != http.StatusUnauthorized {
		t.Errorf("no auth: %d", code)
	}
}

func TestValidation(t *testing.T) {
	var changes atomic.Int32
	store := NewMemoryStore()
	c := newClient(t, APIConfig{Token: token, Store: store, OnChange: func() { changes.Add(1) }})
	for name, body := range map[string]string{
		"empty body":       ``,
		"not json":         `address=` + alice,
		"missing address":  `{}`,
		"empty address":    `{"address":""}`,
		"bad checksum":     `{"address":"TYGjwWR9qhmGuTbCdGvgoM5Yv7rZPeC2yX"}`,
		"too short":        `{"address":"Tnope"}`,
		"hex not accepted": `{"address":"41f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07"}`,
		"evm":              `{"address":"0xf4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07"}`,
		"bad direction":    `{"address":"` + alice + `","direction":"sideways"}`,
		"unknown field":    `{"address":"` + alice + `","enabled":false}`,
		"wrong type":       `{"address":123}`,
		"label too long":   `{"address":"` + alice + `","label":"` + strings.Repeat("x", MaxLabelLen+1) + `"}`,
		"trailing data":    `{"address":"` + alice + `"} {"address":"` + bob + `"}`,
		"oversized body":   `{"address":"` + alice + `","label":"` + strings.Repeat("x", MaxBodyBytes) + `"}`,
	} {
		code, resp, _ := c.do("POST", "/v1/addresses", bearer, body)
		if code != http.StatusBadRequest || !strings.Contains(resp, `"error"`) {
			t.Errorf("%s: %d %s", name, code, resp)
		}
	}
	for _, p := range []string{"/v1/addresses/Tnope", "/v1/addresses/41f4a3aa3c52cdc41e3c1a7b8f7493ae1164ee7f07"} {
		if code, _, _ := c.do("DELETE", p, bearer, ""); code != http.StatusBadRequest {
			t.Errorf("DELETE %s: %d", p, code)
		}
	}
	if all, _ := store.ListWatches(context.Background()); len(all) != 0 || changes.Load() != 0 {
		t.Fatalf("invalid requests changed state: %+v changes=%d", all, changes.Load())
	}
}

func TestCRUD(t *testing.T) {
	var changes atomic.Int32
	store := NewMemoryStore()
	c := newClient(t, APIConfig{Token: token, Store: store, Static: []string{bob}, OnChange: func() { changes.Add(1) }})

	code, body, _ := c.do("GET", "/v1/addresses", bearer, "")
	if code != 200 || strings.TrimSpace(body) != `{"addresses":[],"static_addresses":["`+bob+`"]}` {
		t.Fatalf("empty list: %d %s", code, body)
	}

	code, body, hdr := c.do("POST", "/v1/addresses", bearer, `{"address":" `+alice+` ","label":" order #42 ","direction":"Incoming"}`)
	if code != http.StatusCreated || hdr.Get("Content-Type") != "application/json" {
		t.Fatalf("create: %d %s", code, body)
	}
	var e Entry
	if err := json.Unmarshal([]byte(body), &e); err != nil {
		t.Fatal(err)
	}
	if e.Address != alice || e.Label == nil || *e.Label != "order #42" || e.Direction != "incoming" || !e.Enabled || e.CreatedAt.IsZero() {
		t.Fatalf("created entry %+v", e)
	}
	if changes.Load() != 1 {
		t.Fatalf("OnChange calls = %d, want 1", changes.Load())
	}

	// Defaults: direction both, label null.
	code, body, _ = c.do("POST", "/v1/addresses", bearer, `{"address":"`+bob+`"}`)
	if code != http.StatusCreated || !strings.Contains(body, `"direction":"both"`) || !strings.Contains(body, `"label":null`) {
		t.Fatalf("create bob: %d %s", code, body)
	}

	code, body, _ = c.do("POST", "/v1/addresses", bearer, `{"address":"`+alice+`"}`)
	if code != http.StatusConflict || !strings.Contains(body, alice) {
		t.Fatalf("duplicate: %d %s", code, body)
	}

	code, body, _ = c.do("GET", "/v1/addresses", bearer, "")
	var list listResponse
	if err := json.Unmarshal([]byte(body), &list); err != nil || code != 200 {
		t.Fatalf("list: %d %s %v", code, body, err)
	}
	if len(list.Addresses) != 2 || list.Addresses[0].Address != alice || list.Addresses[1].Address != bob {
		t.Fatalf("list: %+v", list)
	}

	if code, body, _ = c.do("DELETE", "/v1/addresses/"+alice, bearer, ""); code != http.StatusNoContent || body != "" {
		t.Fatalf("delete: %d %s", code, body)
	}
	if code, _, _ = c.do("DELETE", "/v1/addresses/"+alice, bearer, ""); code != http.StatusNotFound {
		t.Fatalf("delete again: %d", code)
	}
	if changes.Load() != 3 {
		t.Fatalf("OnChange calls = %d, want 3 (2 adds + 1 delete)", changes.Load())
	}
	if all, _ := store.ListWatches(context.Background()); len(all) != 1 || all[0].Address != bob {
		t.Fatalf("store after delete: %+v", all)
	}

	// Routing: unknown paths and methods.
	if code, _, _ = c.do("GET", "/v1/nope", bearer, ""); code != http.StatusNotFound {
		t.Errorf("unknown path: %d", code)
	}
	if code, _, _ = c.do("PUT", "/v1/addresses", bearer, `{}`); code != http.StatusMethodNotAllowed {
		t.Errorf("PUT: %d", code)
	}
}

func TestStorageErrorsAre500(t *testing.T) {
	store := NewMemoryStore()
	store.Err = errors.New("connection refused: secret-dsn-detail")
	c := newClient(t, APIConfig{Token: token, Store: store})
	for _, r := range [][3]string{
		{"GET", "/v1/addresses", ""},
		{"POST", "/v1/addresses", `{"address":"` + alice + `"}`},
		{"DELETE", "/v1/addresses/" + alice, ""},
	} {
		code, body, _ := c.do(r[0], r[1], bearer, r[2])
		if code != http.StatusInternalServerError || strings.Contains(body, "secret-dsn-detail") {
			t.Errorf("%s: %d %s (internal details must not leak)", r[0], code, body)
		}
	}
}
