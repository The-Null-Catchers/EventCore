package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestHeaderEncryptionAndValidation(t *testing.T) {
	key := bytes.Repeat([]byte{1}, 32)
	headers := map[string]string{"authorization": "Bearer sensitive", "X-Client": "billing"}
	encrypted, err := EncryptHeaders(key, headers)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(encrypted, "sensitive") {
		t.Fatal("plaintext secret")
	}
	decoded, err := DecryptHeaders(key, encrypted)
	if err != nil || decoded["Authorization"] != "Bearer sensitive" {
		t.Fatal(decoded, err)
	}
	if _, err = Decrypt(key, encrypted); err == nil {
		t.Fatal("encryption purpose not separated")
	}
	for _, bad := range []map[string]string{{"Host": "evil"}, {"x-eVeNtCoRe-Signature": "bad"}, {"X-Test": "line\r\nInjected: yes"}, {"x-test": "1", "X-Test": "2"}, {"Content-Length": "3"}, {"X-Forwarded-For": "1"}, {"Bad Name": "x"}, {"X-Test": strings.Repeat("a", 1025)}} {
		if _, err = NormalizeHeaders(bad); err == nil {
			t.Fatal("accepted", bad)
		}
	}
	if _, err = DecryptHeaders(bytes.Repeat([]byte{2}, 32), encrypted); err == nil {
		t.Fatal("wrong key")
	}
}
func TestRetryAfterBoundaries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	for _, tc := range []struct {
		raw   string
		delay time.Duration
	}{{"60", time.Minute}, {"9999999999", time.Hour}, {"-1", 0}, {"garbage", 0}, {now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second}, {now.Add(-time.Hour).Format(http.TimeFormat), 0}} {
		if delay := RetryAfter(tc.raw, now); delay != tc.delay {
			t.Fatal(tc, delay)
		}
	}
	if RetryDelay(3600, 20) != time.Hour || RetryDelay(2, 3) != 8*time.Second {
		t.Fatal("backoff")
	}
}
func workerFixture(t *testing.T, status int) (*Worker, *memory, *time.Time, *int, storage.Event) {
	t.Helper()
	b, err := storage.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	b.Create(storage.Topic{Workspace: "demo", Name: "orders", Partitions: 1})
	store := &memory{attempts: map[string]Attempt{}}
	g, err := groups.New(b, store, time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	calls := 0
	w := &Worker{Broker: b, Groups: g, Store: store, Key: bytes.Repeat([]byte{2}, 32), Now: func() time.Time { return now }, Client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.Header.Get("Authorization") != "Bearer sensitive" {
			t.Error("header not delivered")
		}
		payload, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-EventCore-Signature") != Sign(strings.Repeat("s", 32), r.Header.Get("X-EventCore-Timestamp"), payload) {
			t.Error("signature changed")
		}
		if _, ok := r.Context().Deadline(); !ok {
			t.Error("unbounded delivery")
		}
		return &http.Response{StatusCode: status, Body: io.NopCloser(bytes.NewBufferString("secret response body")), Header: http.Header{"Retry-After": []string{"60"}}}, nil
	})}}
	if err = w.Initialize(context.Background(), Subscription{Workspace: "demo", Topic: "orders", URL: "https://example.com/hooks", Secret: strings.Repeat("s", 32), Headers: map[string]string{"Authorization": "Bearer sensitive"}, MaxAttempts: 3, DelaySeconds: 1}); err != nil {
		t.Fatal(err)
	}
	if store.subs[0].Headers != nil || store.subs[0].EncryptedHeaders == "" {
		t.Fatal("headers stored in plaintext")
	}
	event, err := b.Publish("demo", "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	return w, store, &now, &calls, event
}
func TestDeliveryClassificationAndHistory(t *testing.T) {
	for _, status := range []int{200, 301, 400, 401, 403, 404, 408, 422, 425, 429, 500, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			w, store, now, calls, event := workerFixture(t, status)
			if err := w.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			a := store.attempts[store.subs[0].ID+event.ID]
			retry := status == 408 || status == 425 || status == 429 || status >= 500
			if status == 200 {
				if a.Status != "delivered" {
					t.Fatal(a)
				}
			} else if retry {
				if a.Status != "retry" {
					t.Fatal(a)
				}
				if status == 429 || status == 503 {
					if a.Next.Sub(*now) != time.Minute {
						t.Fatal(a.Next, *now)
					}
				}
			} else {
				if a.Status != "dlq" || a.Count != 1 {
					t.Fatal(a)
				}
				events, err := w.Broker.Read("demo", "orders.DLQ", 0, 0, 10)
				if err != nil || len(events) != 1 {
					t.Fatal(events, err)
				}
			}
			if len(store.history) < 2 || store.history[0].Status != "sending" || store.history[0].HTTPStatus != 0 || store.history[0].CompletedAt != nil || store.history[1].CompletedAt == nil || store.history[1].StartedAt.IsZero() {
				t.Fatal(store.history)
			}
			raw, _ := json.Marshal(store.history)
			if bytes.Contains(raw, []byte("sensitive")) || bytes.Contains(raw, []byte("secret response")) {
				t.Fatal("secret leaked")
			}
			if err := w.Tick(context.Background()); err != nil {
				t.Fatal(err)
			}
			if *calls != 1 {
				t.Fatal("early retry", *calls)
			}
		})
	}
}
func TestInterruptedDeliveryRemainsObservable(t *testing.T) {
	w, store, now, calls, event := workerFixture(t, 500)
	store.failStatus = "retry"
	if err := w.Tick(context.Background()); err == nil {
		t.Fatal("metadata failure swallowed")
	}
	a := store.attempts[store.subs[0].ID+event.ID]
	if a.Status != "sending" {
		t.Fatal(a)
	}
	store.failStatus = ""
	*now = now.Add(31 * time.Second)
	recovered, err := groups.New(w.Broker, store, time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	w.Groups = recovered
	if err := w.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if *calls != 2 {
		t.Fatal(*calls)
	}
	found := false
	for _, a := range store.history {
		if a.Status == "unknown" && a.Count == 1 {
			found = true
		}
	}
	if !found {
		t.Fatal("interrupted attempt missing", store.history)
	}
}
func TestTransportErrorsDoNotLeakURLSecrets(t *testing.T) {
	w, store, _, _, event := workerFixture(t, 500)
	w.Client = &http.Client{Transport: roundTrip(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("Bearer sensitive https://example.com/?token=secret")
	})}
	if err := w.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	a := store.attempts[store.subs[0].ID+event.ID]
	if a.Error != "transport failure" {
		t.Fatal(a)
	}
}
