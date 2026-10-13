// Package apierr renders the JSON error body shared by every /v1 endpoint:
//
//	{"error": "human readable message", "code": "machine_readable_code"}
//
// "error" keeps its original string type, so the format is a backward
// compatible extension of the v1 API; clients should branch on "code".
package apierr

import (
	"encoding/json"
	"net/http"
)

// Error codes (stable within /v1).
const (
	CodeAPIDisabled      = "api_disabled"
	CodeUnauthorized     = "unauthorized"
	CodePostgresRequired = "postgres_required"
	CodeNotFound         = "not_found"
	CodeMethodNotAllowed = "method_not_allowed"
	CodeInvalidRequest   = "invalid_request"
	CodeChainDisabled    = "chain_disabled"
	CodeConflict         = "conflict"
	CodeInternal         = "internal_error"
)

// Body is the error response.
type Body struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// Write sends a JSON error with status code.
func Write(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(Body{Error: msg, Code: code})
}
