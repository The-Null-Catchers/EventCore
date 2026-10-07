package webhooks

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/The-Null-Catchers/EventCore/internal/deadletters"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"io"
	"net/http"
	"net/netip"
	"testing"
	"time"
)

type memory struct {
	subs     []Subscription
	attempts map[string]Attempt
	groups   []groups.State
	decision deadletters.Decision
	resolved bool
}

func (m *memory) Subscriptions(context.Context) ([]Subscription, error) { return m.subs, nil }
func (m *memory) Create(_ context.Context, s Subscription) error {
	m.subs = append(m.subs, s)
	return nil
}
func (m *memory) Attempt(_ context.Context, s, e string) (Attempt, error) {
	return m.attempts[s+e], nil
}
func (m *memory) SaveAttempt(_ context.Context, s, e string, a Attempt) error {
	m.attempts[s+e] = a
	return nil
}
func (m *memory) Load() ([]groups.State, error) { return m.groups, nil }
func (m *memory) Save(s groups.State) error {
	for i, old := range m.groups {
		if old.Name == s.Name {
			m.groups[i] = s
			return nil
		}
	}
	m.groups = append(m.groups, s)
	return nil
}

func (m *memory) DLQDecision(context.Context, string, string, int, int64) (deadletters.Decision, bool, error) {
	return m.decision, m.resolved, nil
}
func (m *memory) SaveDLQDecision(_ context.Context, d deadletters.Decision) error {
	m.decision = d
	m.resolved = true
	return nil
}
func (m *memory) NextPendingDLQ(context.Context) (deadletters.Decision, bool, error) {
	return m.decision, m.resolved && m.decision.Status == "pending", nil
}

type roundTrip func(*http.Request) (*http.Response, error)

func (f roundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestWebhookRetriesAndDLQ(t *testing.T) {
	b, err := storage.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.Create(storage.Topic{Workspace: "demo", Name: "orders", Partitions: 1})
	store := &memory{attempts: map[string]Attempt{}}
	g, err := groups.New(b, store, time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	gClock := now
	_ = gClock
	key := bytes.Repeat([]byte{42}, 32)
	requests := 0
	w := &Worker{Broker: b, Groups: g, Store: store, Key: key, Now: func() time.Time { return now }, Client: &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		requests++
		payload, _ := io.ReadAll(r.Body)
		if r.Header.Get("X-EventCore-Signature") != Sign("12345678901234567890123456789012", r.Header.Get("X-EventCore-Timestamp"), payload) {
			t.Error("signature invalid")
		}
		return &http.Response{StatusCode: 500, Body: io.NopCloser(bytes.NewBufferString("failure")), Header: make(http.Header)}, nil
	})}}
	if err = w.Initialize(context.Background(), Subscription{Workspace: "demo", Topic: "orders", URL: "https://example.com/webhook", Secret: "12345678901234567890123456789012", MaxAttempts: 3, DelaySeconds: 1}); err != nil {
		t.Fatal(err)
	}
	event, err := b.Publish("demo", "orders", storage.Input{Type: "order.created", Data: json.RawMessage(`{"id":"1"}`)})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err = w.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(10 * time.Second)
	}
	if requests != 3 {
		t.Fatal("attempt count", requests)
	}
	events, err := b.Read("demo", "orders.DLQ", 0, 0, 10)
	if err != nil || len(events) != 1 {
		t.Fatal(events, err)
	}
	var dlq struct {
		OriginalEvent storage.Event `json:"original_event"`
		Attempts      int           `json:"attempts"`
	}
	json.Unmarshal(events[0].Data, &dlq)
	if dlq.OriginalEvent.ID != event.ID || dlq.Attempts != 3 {
		t.Fatal(dlq)
	}
	snap, err := g.Inspect("demo", "orders", "webhook-"+store.subs[0].ID)
	if err != nil || snap.Lag[0] != 0 {
		t.Fatal(snap, err)
	}
	if a := store.attempts[store.subs[0].ID+event.ID]; a.Status != "dlq" || a.HTTPStatus != 500 {
		t.Fatal(a)
	}
	manager := &deadletters.Manager{Broker: b, Store: store}
	decision, err := manager.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "retry")
	if err != nil || decision.Receipt.ID == event.ID {
		t.Fatal(decision, err)
	}
	w.Client = &http.Client{Transport: roundTrip(func(r *http.Request) (*http.Response, error) {
		requests++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewBufferString("ok")), Header: make(http.Header)}, nil
	})}
	if err = w.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a := store.attempts[store.subs[0].ID+decision.Receipt.ID]; a.Status != "delivered" || a.Count != 1 {
		t.Fatal(a)
	}
	if _, err = manager.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "retry"); err != nil {
		t.Fatal(err)
	}
	if requests != 4 {
		t.Fatal("retry repeated delivery", requests)
	}

}
func TestSSRFAndEncryption(t *testing.T) {
	for _, ip := range []string{"127.0.0.1", "10.1.2.3", "169.254.169.254", "::1", "::ffff:127.0.0.1", "100.100.100.200", "2002:7f00:1::"} {
		if PublicIP(netip.MustParseAddr(ip)) {
			t.Error("accepted", ip)
		}
	}
	if !PublicIP(netip.MustParseAddr("8.8.8.8")) {
		t.Fatal("public IP rejected")
	}
	for _, url := range []string{"http://example.com", "https://127.0.0.1", "https://user:password@example.com", "https://example.com:8080"} {
		if ValidateURL(url) == nil {
			t.Error("URL accepted", url)
		}
	}
	key := bytes.Repeat([]byte{3}, 32)
	encrypted, err := Encrypt(key, "secret")
	if err != nil {
		t.Fatal(err)
	}
	plain, err := Decrypt(key, encrypted)
	if err != nil || plain != "secret" {
		t.Fatal(plain, err)
	}
	if _, err = Decrypt(bytes.Repeat([]byte{4}, 32), encrypted); err == nil {
		t.Fatal("wrong key accepted")
	}
}
