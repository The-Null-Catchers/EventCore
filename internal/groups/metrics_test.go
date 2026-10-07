package groups

import (
	"encoding/json"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"testing"
	"time"
)

func TestMetricsReflectLagLeasesAndExpiryWithoutMutation(t *testing.T) {
	c, _, store := setup(t)
	now := time.Now()
	c.now = func() time.Time { return now }
	snap, err := c.Join("demo", "orders", "billing", "a")
	if err != nil {
		t.Fatal(err)
	}
	deliveries, err := c.Pull("demo", "orders", "billing", "a", snap.Epoch, 2)
	if err != nil {
		t.Fatal(err)
	}
	m, err := c.Metrics("demo")
	if err != nil || len(m) != 1 || m[0].Members != 1 || m[0].Consumed != 8 {
		t.Fatal(m, err)
	}
	sum := int64(0)
	for _, p := range m[0].Partitions {
		sum += p.Lag
		if !p.InFlight {
			t.Fatal(p)
		}
	}
	if sum != 40 {
		t.Fatal(sum)
	}
	d := deliveries[0]
	if err = c.Commit("demo", "orders", "billing", "a", d.Epoch, d.Partition, d.Token, 1); err != nil {
		t.Fatal(err)
	}
	m, _ = c.Metrics("demo")
	sum = 0
	for _, p := range m[0].Partitions {
		sum += p.Lag
	}
	if sum != 39 {
		t.Fatal(sum)
	}
	epoch := store.states[groupKey("demo", "orders", "billing")].Epoch
	store.fail = true
	now = now.Add(2 * time.Minute)
	m, err = c.Metrics("demo")
	if err != nil || m[0].Members != 0 {
		t.Fatal(m, err)
	}
	for _, p := range m[0].Partitions {
		if p.InFlight {
			t.Fatal("expired lease counted")
		}
	}
	if store.states[groupKey("demo", "orders", "billing")].Epoch != epoch {
		t.Fatal("scrape mutated generation")
	}
	other, err := c.Metrics("other")
	if err != nil || len(other) != 0 {
		t.Fatal(other, err)
	}
}

func TestMetricsExposeOffsetsLostToRetention(t *testing.T) {
	c, b, _ := setup(t)
	if err := b.Create(storage.Topic{Workspace: "demo", Name: "retained", Partitions: 1, RetentionBytes: 1024}); err != nil {
		t.Fatal(err)
	}
	if err := c.Create("demo", "retained", "slow", "earliest"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if _, err := b.Publish("demo", "retained", storage.Input{Type: "event", Data: json.RawMessage(`{}`)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Retain(time.Now()); err != nil {
		t.Fatal(err)
	}
	bounds, _ := b.Bounds("demo", "retained", 0)
	if bounds.Oldest == 0 {
		t.Fatal("fixture did not expire")
	}
	metrics, err := c.Metrics("demo")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range metrics {
		if m.Topic == "retained" {
			found = true
			v := m.Partitions[0]
			if v.Lag != 20 || v.Unavailable != bounds.Oldest || v.Committed != 0 {
				t.Fatal(v, bounds)
			}
		}
	}
	if !found {
		t.Fatal("missing group")
	}
}
