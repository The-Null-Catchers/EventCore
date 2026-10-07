package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestIdempotentPublishConcurrentRecovery(t *testing.T) {
	b, dir := fixture(t, 4)
	in := input("")
	in.IdempotencyKey = "request-123"
	var wg sync.WaitGroup
	results := make(chan Event, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			e, err := b.Publish("demo", "orders", in)
			if err != nil {
				t.Error(err)
				return
			}
			results <- e
		}()
	}
	wg.Wait()
	close(results)
	var original Event
	appended := 0
	for e := range results {
		if original.ID == "" {
			original = e
		}
		if e.ID != original.ID || e.Partition != original.Partition || e.Offset != original.Offset {
			t.Fatal("retry changed receipt", e, original)
		}
		if !e.Deduplicated {
			appended++
		}
	}
	if appended != 1 {
		t.Fatal("concurrent requests appended", appended)
	}
	// Other writes rotate segments without retaining entire event payloads in the index.
	for i := 0; i < 20; i++ {
		if _, err := b.Publish("demo", "orders", input("")); err != nil {
			t.Fatal(err)
		}
	}
	b.Close()
	recovered, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	duplicate, err := recovered.Publish("demo", "orders", in)
	if err != nil || !duplicate.Deduplicated || duplicate.ID != original.ID || duplicate.Offset != original.Offset || duplicate.Partition != original.Partition {
		t.Fatal(duplicate, err)
	}
	var count int64
	for p := 0; p < 4; p++ {
		bounds, _ := recovered.Bounds("demo", "orders", p)
		count += bounds.Events
	}
	if count != 21 {
		t.Fatal("duplicate appended after recovery", count)
	}
	stored, err := recovered.Read("demo", "orders", original.Partition, original.Offset, 1)
	if err != nil || stored[0].Deduplicated {
		t.Fatal("response-only flag persisted", stored, err)
	}
}

func TestIdempotencyConflictScopeAndValidation(t *testing.T) {
	b, _ := fixture(t, 4)
	in := Input{Type: "created", Key: "customer", Data: json.RawMessage(`{"a":1234567890123456789,"b":2}`), IdempotencyKey: "req"}
	original, err := b.Publish("demo", "orders", in)
	if err != nil {
		t.Fatal(err)
	}
	equivalent := in
	equivalent.Data = json.RawMessage(`{ "b": 2, "a": 1234567890123456789 }`)
	equivalent.Headers = map[string]string{}
	e, err := b.Publish("demo", "orders", equivalent)
	if err != nil || e.ID != original.ID || !e.Deduplicated {
		t.Fatal(e, err)
	}
	for _, variant := range []string{"type", "key", "data", "headers"} {
		changed := in
		switch variant {
		case "type":
			changed.Type = "updated"
		case "key":
			changed.Key = "different-customer"
		case "data":
			changed.Data = json.RawMessage(`{"a":1234567890123456790,"b":2}`)
		case "headers":
			changed.Headers = map[string]string{"source": "changed"}
		}
		if _, err = b.Publish("demo", "orders", changed); !errors.Is(err, ErrIdempotencyConflict) {
			t.Fatal(variant, err)
		}
	}
	for _, cfg := range []Topic{{Workspace: "other", Name: "orders", Partitions: 4}, {Workspace: "demo", Name: "payments", Partitions: 4}} {
		if err = b.Create(cfg); err != nil {
			t.Fatal(err)
		}
		e, err = b.Publish(cfg.Workspace, cfg.Name, in)
		if err != nil || e.ID == original.ID || e.Deduplicated {
			t.Fatal("scope collision", e, err)
		}
	}
	invalid := in
	invalid.IdempotencyKey = strings.Repeat("x", 257)
	if _, err = b.Publish("demo", "orders", invalid); err == nil {
		t.Fatal("unbounded key accepted")
	}
	// Existing receipts remain available at the admission threshold; new writes stop.
	disk, _ := b.Disk()
	b.MinFreeBytes = disk.TotalBytes + 1
	if e, err = b.Publish("demo", "orders", in); err != nil || e.ID != original.ID {
		t.Fatal(e, err)
	}
	invalid.IdempotencyKey = "new"
	if _, err = b.Publish("demo", "orders", invalid); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
}

func TestIdempotencyRetentionAndPartitionFailure(t *testing.T) {
	b, _ := fixture(t, 1)
	in := input("")
	in.IdempotencyKey = "retained"
	original, err := b.Publish("demo", "orders", in)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		b.Publish("demo", "orders", input(""))
	}
	topic := b.topics[key("demo", "orders")]
	topic.config.RetentionSeconds = 1
	if err = b.Retain(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	e, err := b.Publish("demo", "orders", in)
	if err != nil || e.Deduplicated || e.Offset <= original.Offset {
		t.Fatal("removed receipt was reused", e, err)
	}
	p := topic.parts[e.Partition]
	p.poisoned = errors.New("injected fsync failure")
	if _, err = b.Publish("demo", "orders", in); !errors.Is(err, ErrUnavailable) {
		t.Fatal("fenced partition reported success", err)
	}
}

func TestIdempotencyBoundedIndexAndExpiry(t *testing.T) {
	var d dedupIndex
	now := time.Now().UTC()
	for i := 0; i < IdempotencyCapacity+20; i++ {
		e := Event{ID: fmt.Sprint(i), Offset: int64(i), Timestamp: now.Add(time.Duration(i) * time.Millisecond), Input: input("")}
		e.IdempotencyKey = fmt.Sprint(i)
		if err := d.remember(e, now); err != nil {
			t.Fatal(err)
		}
	}
	if len(d.byKey) != IdempotencyCapacity || len(d.oldest) != IdempotencyCapacity || d.byKey["0"] != nil || d.byKey["20"] == nil {
		t.Fatal("index not bounded")
	}
	// Input scan order during recovery must not change which receipts survive.
	var reverse dedupIndex
	for i := IdempotencyCapacity + 19; i >= 0; i-- {
		e := Event{ID: fmt.Sprint(i), Offset: int64(i), Timestamp: now.Add(time.Duration(i) * time.Millisecond), Input: input("")}
		e.IdempotencyKey = fmt.Sprint(i)
		reverse.remember(e, now)
	}
	for k := range d.byKey {
		if reverse.byKey[k] == nil {
			t.Fatal("recovery eviction changed", k)
		}
	}
	d.prune(now.Add(IdempotencyWindow + time.Minute))
	if len(d.byKey) != 0 || len(d.oldest) != 0 {
		t.Fatal("expired receipts retained")
	}
}
