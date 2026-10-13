// Package httpx holds small HTTP helpers shared by the chain clients and
// sinks: URL redaction, error scrubbing, safe body handling and a client
// that never forwards credentials across hosts on redirects.
package httpx

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// RedactURL returns scheme://host of raw, hiding paths, query strings and
// userinfo that commonly carry API keys or tokens.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "<invalid url>"
	}
	r := u.Scheme + "://" + u.Host
	if (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.User != nil {
		r += "/***"
	}
	return r
}

// ScrubURLError strips the request URL from a *url.Error (net/http client
// errors embed the full URL, which may contain secrets) and returns the
// underlying cause.
func ScrubURLError(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err
	}
	return err
}

// maxRedirects bounds same-host redirect chains.
const maxRedirects = 5

// NewClient returns an *http.Client with timeout that follows redirects only
// within the original scheme+host. Clients here send credentials in custom
// headers (TRON-PRO-API-KEY, X-API-Key) or the URL itself, which net/http
// would forward to any host a server redirects to; cross-host redirects and
// https→http downgrades are refused instead.
func NewClient(timeout time.Duration) *http.Client {
	return &http.Client{Timeout: timeout, CheckRedirect: SameHostRedirects}
}

// SameHostRedirects is an http.Client.CheckRedirect policy (see NewClient).
func SameHostRedirects(req *http.Request, via []*http.Request) error {
	if len(via) >= maxRedirects {
		return fmt.Errorf("stopped after %d redirects", maxRedirects)
	}
	orig := via[0].URL
	if req.URL.Host != orig.Host || (orig.Scheme == "https" && req.URL.Scheme != "https") {
		return fmt.Errorf("refusing redirect from %s to another host/scheme (%s://%s): credentials would leak", orig.Host, req.URL.Scheme, req.URL.Host)
	}
	return nil
}

// ReadBody reads at most limit bytes of resp.Body, drains a bounded amount
// of the rest (so the connection can be reused) and closes it.
func ReadBody(resp *http.Response, limit int64) ([]byte, error) {
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		_ = resp.Body.Close()
	}()
	return io.ReadAll(io.LimitReader(resp.Body, limit))
}
