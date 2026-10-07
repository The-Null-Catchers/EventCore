package deadletters

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"sync"
	"testing"
	"time"
)

type memoryStore struct {
	decisions    map[int64][]byte
	failComplete bool
	ambiguous    bool
}

func (s *memoryStore) DLQDecision(_ context.Context, w, topic string, p int, offset int64) (Decision, bool, error) {
	var d Decision
	raw, ok := s.decisions[offset]
	if ok {
		_ = json.Unmarshal(raw, &d)
		if d.Workspace != w || d.Topic != topic || d.Partition != p {
			return Decision{}, false, nil
		}
	}
	return d, ok, nil
}
func (s *memoryStore) SaveDLQDecision(_ context.Context, d Decision) error {
	if s.failComplete && d.Status == "complete" {
		return errors.New("database offline")
	}
	raw, _ := json.Marshal(d)
	s.decisions[d.Offset] = raw
	if s.ambiguous && d.Status == "pending" {
		return errors.New("unknown commit outcome")
	}
	return nil
}
func (s *memoryStore) NextPendingDLQ(context.Context) (Decision, bool, error) {
	for _, raw := range s.decisions {
		var d Decision
		json.Unmarshal(raw, &d)
		if d.Status == "pending" {
			return d, true, nil
		}
	}
	return Decision{}, false, nil
}
func fixture(t *testing.T) (*Manager, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := storage.Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	for _, n := range []string{"orders", "orders.DLQ"} {
		if err = b.Create(storage.Topic{Workspace: "demo", Name: n, Partitions: 1, RetentionBytes: 512}); err != nil {
			t.Fatal(err)
		}
	}
	original, err := b.Publish("demo", "orders", storage.Input{Type: "order.created", Data: json.RawMessage(`{"id":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(Failure{OriginalEvent: original, OriginalTopic: "orders", OriginalPartition: 0, OriginalOffset: 0, SubscriptionID: "hook", Attempts: 3, FailedAt: time.Now()})
	_, err = b.Publish("demo", "orders.DLQ", storage.Input{Type: "eventcore.delivery.failed", Data: raw})
	if err != nil {
		t.Fatal(err)
	}
	return &Manager{Broker: b, Store: &memoryStore{decisions: map[int64][]byte{}}}, dir
}
func TestResolutionConcurrentRetryAndDiscard(t *testing.T) {
	m, _ := fixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d, err := m.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "retry")
			if err != nil || d.Status != "complete" || d.Receipt.Offset != 1 || d.Candidate != nil {
				t.Errorf("%+v %v", d, err)
			}
		}()
	}
	wg.Wait()
	bounds, _ := m.Broker.Bounds("demo", "orders", 0)
	if bounds.Next != 2 {
		t.Fatal(bounds)
	}
	if _, err := m.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "discard"); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	records, _ := m.Broker.Read("demo", "orders", 0, 1, 1)
	if records[0].Headers["eventcore.dlq.original_id"] == "" {
		t.Fatal(records)
	}
	dlq, _ := m.Broker.Read("demo", "orders.DLQ", 0, 0, 1)
	if len(dlq) != 1 {
		t.Fatal("source mutated")
	}
	m2, _ := fixture(t)
	d, err := m2.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "discard")
	if err != nil || d.Status != "complete" || d.Receipt != nil {
		t.Fatal(d, err)
	}
	bounds, _ = m2.Broker.Bounds("demo", "orders", 0)
	if bounds.Next != 1 {
		t.Fatal(bounds)
	}
}
func TestRecoverAfterAppendAndRetentionPin(t *testing.T) {
	m, dir := fixture(t)
	store := m.Store.(*memoryStore)
	store.failComplete = true
	if _, err := m.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "retry"); !errors.Is(err, ErrStore) {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := m.Broker.Publish("demo", "orders", storage.Input{Type: "more", Data: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := m.Broker.Retain(time.Now()); err != nil {
		t.Fatal(err)
	}
	bounds, _ := m.Broker.Bounds("demo", "orders", 0)
	if bounds.Oldest > 1 {
		t.Fatal("pending receipt expired", bounds)
	}
	m.Broker.Close()
	b, err := storage.Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	m.Broker = b
	store.failComplete = false
	if err = m.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	bounds, _ = b.Bounds("demo", "orders", 0)
	if bounds.Next != 12 {
		t.Fatal("duplicate retry", bounds)
	}
	d, _, _ := store.DLQDecision(context.Background(), "demo", "orders.DLQ", 0, 0)
	if d.Candidate != nil || d.Status != "complete" {
		t.Fatal(d)
	}
	if err = b.Retain(time.Now()); err != nil {
		t.Fatal(err)
	}
	bounds, _ = b.Bounds("demo", "orders", 0)
	if bounds.Oldest <= 1 {
		t.Fatal("pin not released", bounds)
	}
	if _, err = m.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "retry"); err != nil {
		t.Fatal(err)
	}
}
func TestAmbiguousPrepareFencesUntilRecovery(t *testing.T) {
	m, dir := fixture(t)
	store := m.Store.(*memoryStore)
	store.ambiguous = true
	if _, err := m.Resolve(context.Background(), "demo", "orders.DLQ", "owner", 0, 0, "retry"); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatal(err)
	}
	if _, err := m.Broker.Publish("demo", "orders", storage.Input{Type: "other", Data: json.RawMessage(`{}`)}); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatal("reservation was overwritten", err)
	}
	m.Broker.Close()
	b, err := storage.Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	m.Broker = b
	store.ambiguous = false
	if err = m.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	bounds, _ := b.Bounds("demo", "orders", 0)
	if bounds.Next != 2 {
		t.Fatal(bounds)
	}
}
func TestRecoveryRejectsDifferentRecord(t *testing.T) {
	m, _ := fixture(t)
	original, _ := m.Broker.Read("demo", "orders", 0, 0, 1)
	candidate := original[0]
	candidate.ID = storage.ID()
	store := m.Store.(*memoryStore)
	store.SaveDLQDecision(context.Background(), Decision{Workspace: "demo", Topic: "orders.DLQ", Action: "retry", Status: "pending", Candidate: &candidate})
	if err := m.Recover(context.Background()); !errors.Is(err, storage.ErrUnavailable) {
		t.Fatal(err)
	}
	bounds, _ := m.Broker.Bounds("demo", "orders", 0)
	if bounds.Next != 1 {
		t.Fatal(bounds)
	}
}
