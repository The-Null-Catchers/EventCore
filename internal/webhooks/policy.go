package webhooks

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// NormalizeHeaders rejects ambiguous names, injection, routing overrides and
// platform-controlled fields. Authorization values are permitted but encrypted.
func NormalizeHeaders(headers map[string]string) (map[string]string, error) {
	if len(headers) > 16 {
		return nil, errors.New("maximum 16 custom headers")
	}
	out := map[string]string{}
	total := 0
	for name, value := range headers {
		if len(name) == 0 || len(name) > 64 || len(value) > 1024 {
			return nil, errors.New("header name/value exceeds limit")
		}
		for _, c := range name {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
				return nil, errors.New("invalid header name")
			}
		}
		for _, c := range value {
			if c != '\t' && (c < 32 || c > 126) {
				return nil, errors.New("header values must be printable ASCII")
			}
		}
		key := http.CanonicalHeaderKey(name)
		lower := strings.ToLower(name)
		switch lower {
		case "host", "content-type", "content-length", "content-encoding", "connection", "transfer-encoding", "trailer", "te", "upgrade", "expect", "proxy-authorization", "proxy-authenticate", "forwarded", "user-agent":
			return nil, errors.New("reserved custom header")
		}
		if strings.HasPrefix(lower, "x-eventcore-") || strings.HasPrefix(lower, "x-forwarded-") {
			return nil, errors.New("reserved custom header")
		}
		if _, ok := out[key]; ok {
			return nil, errors.New("duplicate case-insensitive custom header")
		}
		out[key] = value
		total += len(name) + len(value)
	}
	if total > 8192 {
		return nil, errors.New("custom headers exceed 8 KiB")
	}
	return out, nil
}
func HeaderNames(headers map[string]string) []string {
	out := []string{}
	for k := range headers {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
func EncryptHeaders(key []byte, headers map[string]string) (string, error) {
	normalized, err := NormalizeHeaders(headers)
	if err != nil {
		return "", err
	}
	raw, err := json.Marshal(normalized)
	if err != nil {
		return "", err
	}
	return encrypt(key, string(raw), "eventcore.webhook.headers.v1")
}
func DecryptHeaders(key []byte, encrypted string) (map[string]string, error) {
	if encrypted == "" {
		return map[string]string{}, nil
	}
	raw, err := decrypt(key, encrypted, "eventcore.webhook.headers.v1")
	if err != nil {
		return nil, errors.New("custom header decryption failed")
	}
	if len(raw) > 16384 {
		return nil, errors.New("encrypted headers exceed limit")
	}
	var headers map[string]string
	if json.Unmarshal([]byte(raw), &headers) != nil {
		return nil, errors.New("invalid encrypted headers")
	}
	return NormalizeHeaders(headers)
}
func RetryDelay(seconds, count int) time.Duration {
	delay := time.Duration(seconds) * time.Second
	for n := 1; n < count && delay < time.Hour; n++ {
		delay *= 2
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}
func RetryAfter(raw string, now time.Time) time.Duration {
	var delay time.Duration
	if seconds, err := strconv.ParseInt(strings.TrimSpace(raw), 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds >= 3600 {
			return time.Hour
		}
		delay = time.Duration(seconds) * time.Second
	} else if date, err := http.ParseTime(raw); err == nil {
		delay = date.Sub(now)
	}
	if delay < 0 {
		return 0
	}
	if delay > time.Hour {
		return time.Hour
	}
	return delay
}

type HistoryEntry struct {
	Cursor     int64     `json:"cursor"`
	EventID    string    `json:"event_id"`
	Attempt    Attempt   `json:"attempt"`
	RecordedAt time.Time `json:"recorded_at"`
}
type HistoryPage struct {
	Entries    []HistoryEntry `json:"entries"`
	NextCursor int64          `json:"next_cursor"`
	HasMore    bool           `json:"has_more"`
}
