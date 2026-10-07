package groups

import (
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"sync"
	"testing"
	"time"
)

type memoryStore struct {
	states map[string]State
	fail   bool
}

func (s *memoryStore) Load() ([]State, error) {
	out := []State{}
	for _, v := range s.states {
		out = append(out, clone(v))
	}
	return out, nil
}
func (s *memoryStore) Save(v State) error {
	if s.fail {
		return errors.New("database unavailable")
	}
	s.states[groupKey(v.Workspace, v.Topic, v.Name)] = clone(v)
	return nil
}
func setup(t *testing.T) (*Coordinator, *storage.Broker, *memoryStore) {
	b, err := storage.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	if err = b.Create(storage.Topic{Workspace: "demo", Name: "orders", Partitions: 4}); err != nil {
		t.Fatal(err)
	}
	s := &memoryStore{states: map[string]State{}}
	c, err := New(b, s, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.Create("demo", "orders", "billing", "earliest"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		if _, err = b.Publish("demo", "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	return c, b, s
}
func TestAssignmentsFencingCommitRestart(t *testing.T) {
	c, b, s := setup(t)
	a, _ := c.Join("demo", "orders", "billing", "a")
	both, err := c.Join("demo", "orders", "billing", "b")
	if err != nil {
		t.Fatal(err)
	}
	if len(both.Members) != 2 || len(both.Members[0].Partitions) != 2 || len(both.Members[1].Partitions) != 2 {
		t.Fatal(both)
	}
	if _, err = c.Pull("demo", "orders", "billing", "a", a.Epoch, 10); err != ErrFenced {
		t.Fatal(err)
	}
	batch, err := c.Pull("demo", "orders", "billing", "a", both.Epoch, 10)
	if err != nil || len(batch) != 2 {
		t.Fatal(batch, err)
	}
	for _, d := range batch {
		if err = c.Ack("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, false); err != nil {
			t.Fatal(err)
		}
	}
	c, err = New(b, s, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	state, _ := c.Inspect("demo", "orders", "billing")
	if state.Offsets[0] != 10 || state.Offsets[2] != 10 || state.Lag[0] != 0 || len(state.Members) != 0 {
		t.Fatal(state)
	}
	if err = c.Reset("demo", "orders", "billing", 0, 0); err != nil {
		t.Fatal(err)
	}
	state, _ = c.Inspect("demo", "orders", "billing")
	if state.Lag[0] != 10 {
		t.Fatal(state)
	}
}
func TestNackLeaseExpiryAndRebalance(t *testing.T) {
	c, _, _ := setup(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	s, _ := c.Join("demo", "orders", "billing", "a")
	d, _ := c.Pull("demo", "orders", "billing", "a", s.Epoch, 2)
	if err := c.Ack("demo", "orders", "billing", "a", s.Epoch, d[0].Partition, d[0].Token, true); err != nil {
		t.Fatal(err)
	}
	retry, _ := c.Pull("demo", "orders", "billing", "a", s.Epoch, 2)
	if retry[0].Events[0].Offset != d[0].Events[0].Offset {
		t.Fatal("nack lost event")
	}
	now = now.Add(2 * time.Second)
	again, _ := c.Pull("demo", "orders", "billing", "a", s.Epoch, 2)
	if err := c.Ack("demo", "orders", "billing", "a", s.Epoch, retry[0].Partition, retry[0].Token, false); err != ErrFenced {
		t.Fatal(err)
	}
	if len(again) == 0 {
		t.Fatal("visibility did not redeliver")
	}
	other, _ := c.Join("demo", "orders", "billing", "b")
	c.Leave("demo", "orders", "billing", "a")
	latest, _ := c.Inspect("demo", "orders", "billing")
	if len(latest.Members[0].Partitions) != 4 || latest.Epoch == other.Epoch {
		t.Fatal(latest)
	}
	if err := c.Reset("demo", "orders", "billing", 0, 0); err == nil {
		t.Fatal("active reset allowed")
	}
}
func TestFailedCommitDoesNotAdvance(t *testing.T) {
	c, _, s := setup(t)
	state, _ := c.Join("demo", "orders", "billing", "a")
	d, _ := c.Pull("demo", "orders", "billing", "a", state.Epoch, 10)
	s.fail = true
	if err := c.Ack("demo", "orders", "billing", "a", state.Epoch, d[0].Partition, d[0].Token, false); err == nil {
		t.Fatal("failed commit accepted")
	}
	state, _ = c.Inspect("demo", "orders", "billing")
	if state.Offsets[d[0].Partition] != 0 {
		t.Fatal("advanced after failure")
	}
	s.fail = false
	if err := c.Ack("demo", "orders", "billing", "a", state.Epoch, d[0].Partition, d[0].Token, false); err != nil {
		t.Fatal(err)
	}
}
func TestConcurrentJoins(t *testing.T) {
	c, _, _ := setup(t)
	var wg sync.WaitGroup
	for _, id := range []string{"a", "b", "c", "d"} {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, err := c.Join("demo", "orders", "billing", id); err != nil {
				t.Error(err)
			}
		}(id)
	}
	wg.Wait()
	state, _ := c.Inspect("demo", "orders", "billing")
	seen := map[int]bool{}
	for _, m := range state.Members {
		for _, p := range m.Partitions {
			if seen[p] {
				t.Fatal("double ownership")
			}
			seen[p] = true
		}
	}
	if len(seen) != 4 {
		t.Fatal(seen)
	}
}

func TestPartialCommitRecoveryAndRedelivery(t *testing.T) {
	c, b, store := setup(t)
	state, _ := c.Join("demo", "orders", "billing", "a")
	batches, _ := c.Pull("demo", "orders", "billing", "a", state.Epoch, 10)
	d := batches[0]
	for _, offset := range []int64{-1, 0, 11} {
		if err := c.Commit("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, offset); err == nil {
			t.Fatalf("invalid commit %d accepted", offset)
		}
	}
	store.fail = true
	if err := c.Commit("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, 3); err == nil {
		t.Fatal("failed metadata commit accepted")
	}
	state, _ = c.Inspect("demo", "orders", "billing")
	if state.Offsets[d.Partition] != 0 {
		t.Fatal("advanced on failure")
	}
	store.fail = false
	if err := c.Commit("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, 3); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, false); err != ErrFenced {
		t.Fatal("old token usable", err)
	}
	remaining, err := c.Pull("demo", "orders", "billing", "a", d.Epoch, 10)
	if err != nil || len(remaining) != 1 || remaining[0].Token == d.Token || remaining[0].Events[0].Offset != 3 || len(remaining[0].Events) != 7 {
		t.Fatal("suffix was not redelivered", remaining, err)
	}
	// Restart before acknowledging the suffix: only the persisted prefix survives.
	restarted, err := New(b, store, time.Minute, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	state, _ = restarted.Join("demo", "orders", "billing", "a")
	if state.Offsets[d.Partition] != 3 || state.Lag[d.Partition] != 7 {
		t.Fatal(state)
	}
	if err := restarted.Commit("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, 4); err != ErrFenced {
		t.Fatal(err)
	}
	recovered, _ := restarted.Pull("demo", "orders", "billing", "a", state.Epoch, 10)
	for _, delivery := range recovered {
		if err := restarted.Commit("demo", "orders", "billing", "a", delivery.Epoch, delivery.Partition, delivery.Token, delivery.Events[len(delivery.Events)-1].Offset+1); err != nil {
			t.Fatal(err)
		}
	}
	state, _ = restarted.Inspect("demo", "orders", "billing")
	if state.Lag[d.Partition] != 0 {
		t.Fatal(state)
	}
}

func TestPartialCommitFencing(t *testing.T) {
	for _, scenario := range []string{"member", "token", "partition", "epoch", "expiry", "rebalance", "member expiry"} {
		t.Run(scenario, func(t *testing.T) {
			c, _, _ := setup(t)
			now := time.Now()
			c.now = func() time.Time { return now }
			s, _ := c.Join("demo", "orders", "billing", "a")
			ds, _ := c.Pull("demo", "orders", "billing", "a", s.Epoch, 10)
			d := ds[0]
			member, token, partition, epoch := "a", d.Token, d.Partition, d.Epoch
			switch scenario {
			case "member":
				member = "b"
			case "token":
				token = "wrong"
			case "partition":
				partition = 99
			case "epoch":
				epoch++
			case "expiry":
				now = now.Add(2 * time.Second)
			case "rebalance":
				c.Join("demo", "orders", "billing", "b")
			case "member expiry":
				now = now.Add(2 * time.Minute)
			}
			if err := c.Commit("demo", "orders", "billing", member, epoch, partition, token, 3); err != ErrFenced {
				t.Fatal(err)
			}
			s, _ = c.Inspect("demo", "orders", "billing")
			if s.Offsets[d.Partition] != 0 {
				t.Fatal("stale consumer advanced offset")
			}
		})
	}
}
