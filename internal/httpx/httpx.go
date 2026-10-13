// Package httpx holds small HTTP helpers shared by the chain clients and
// sinks: URL redaction, error scrubbing, safe body handling and a client
// that never forwards credentials across hosts on redirects.
package httpx

import (
	"errors"
	"net/url"
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
