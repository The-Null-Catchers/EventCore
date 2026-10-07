package api

import (
	"context"
	"encoding/json"
	"github.com/The-Null-Catchers/EventCore/internal/deadletters"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"testing"
)

type decisionStore struct {
	value deadletters.Decision
	found bool
}

func (s *decisionStore) DLQDecision(_ context.Context, w, topic string, p int, offset int64) (deadletters.Decision, bool, error) {
	return s.value, s.found && s.value.Workspace == w, nil
}
func (s *decisionStore) SaveDLQDecision(_ context.Context, v deadletters.Decision) error {
	s.value = v
	s.found = true
	return nil
}
func (s *decisionStore) NextPendingDLQ(context.Context) (deadletters.Decision, bool, error) {
	return s.value, s.found && s.value.Status == "pending", nil
}
func TestDeadLettersPermissionsConfirmationAndAudit(t *testing.T) {
	_, b := apiFixture(t)
	b.Create(storage.Topic{Workspace: "demo", Name: "orders.DLQ", Partitions: 1})
	original, _ := b.Publish("demo", "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)})
	raw, _ := json.Marshal(deadletters.Failure{OriginalEvent: original, OriginalTopic: "orders", OriginalPartition: original.Partition, OriginalOffset: original.Offset, SubscriptionID: "hook", Attempts: 3})
	b.Publish("demo", "orders.DLQ", storage.Input{Type: "eventcore.delivery.failed", Data: raw})
	s := &Server{Broker: b, Auth: testAuth{}, DeadLetters: &deadletters.Manager{Broker: b, Store: &decisionStore{}}}
	path := "/v1/topics/orders.DLQ/dead-letters/retry"
	body := `{"partition":0,"offset":0,"confirm":true}`
	for _, token := range []string{"produce", "consume", "read", "dlq-admin"} {
		w := request(s.Handler(), "POST", path, token, body)
		if w.Code != 403 {
			t.Fatal(token, w.Code, w.Body)
		}
	}
	w := request(s.Handler(), "POST", path, "admin", `{"confirm":true}`)
	if w.Code != 400 {
		t.Fatal(w.Code, w.Body)
	}
	s.Auth = unavailableAudit{}
	w = request(s.Handler(), "POST", path, "admin", body)
	if w.Code != 503 {
		t.Fatal(w.Code, w.Body)
	}
	if s.DeadLetters.Store.(*decisionStore).found {
		t.Fatal("acted without audit")
	}
	s.Auth = testAuth{}
	for i := 0; i < 2; i++ {
		w = request(s.Handler(), "POST", path, "admin", body)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body)
		}
	}
	w = request(s.Handler(), "POST", "/v1/topics/orders.DLQ/dead-letters/discard", "admin", body)
	if w.Code != 409 {
		t.Fatal(w.Code, w.Body)
	}
	w = request(s.Handler(), "GET", "/v1/topics/orders.DLQ/dead-letters?partition=0&offset=0", "admin", "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	var page map[string]any
	json.Unmarshal(w.Body.Bytes(), &page)
	if len(page["entries"].([]any)) != 1 {
		t.Fatal(page)
	}
	w = request(s.Handler(), "POST", path, "other", body)
	if w.Code == 200 {
		t.Fatal("cross-workspace resolution")
	}
}
