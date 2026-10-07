package metadata

import (
	"context"
	"encoding/json"
	"github.com/The-Null-Catchers/EventCore/internal/deadletters"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"os"
	"testing"
)

func TestPostgresDLQRecovery(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	ctx := context.Background()
	db, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.SQL.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	w := "dlq-" + storage.ID()
	if _, err = db.SQL.ExecContext(ctx, `INSERT INTO workspaces(id) VALUES($1)`, w); err != nil {
		t.Fatal(err)
	}
	defer db.SQL.ExecContext(ctx, `DELETE FROM dlq_resolutions WHERE workspace_id=$1`, w)
	b, err := storage.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, topic := range []string{"orders", "orders.DLQ"} {
		if err = b.Create(storage.Topic{Workspace: w, Name: topic, Partitions: 1}); err != nil {
			t.Fatal(err)
		}
	}
	e, err := b.Publish(w, "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(deadletters.Failure{OriginalEvent: e, OriginalTopic: "orders", SubscriptionID: "hook", Attempts: 3})
	b.Publish(w, "orders.DLQ", storage.Input{Type: "eventcore.delivery.failed", Data: raw})
	m := &deadletters.Manager{Broker: b, Store: db}
	d, err := m.Resolve(ctx, w, "orders.DLQ", "owner", 0, 0, "retry")
	if err != nil || d.Receipt.Offset != 1 {
		t.Fatal(d, err)
	}
	again, err := m.Resolve(ctx, w, "orders.DLQ", "owner", 0, 0, "retry")
	if err != nil || again.Receipt.ID != d.Receipt.ID {
		t.Fatal(again, err)
	}
	candidate, err := b.PublishPrepared(w, "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)}, func(e storage.Event) error {
		return db.SaveDLQDecision(ctx, deadletters.Decision{Workspace: w, Topic: "orders.DLQ", Offset: 1, Action: "retry", Status: "pending", Candidate: &e})
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = m.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	pending, ok, err := db.NextPendingDLQ(ctx)
	if err != nil || ok {
		t.Fatal(pending, ok, err)
	}
	saved, ok, err := db.DLQDecision(ctx, w, "orders.DLQ", 0, 1)
	if err != nil || !ok || saved.Candidate != nil || saved.Receipt.ID != candidate.ID {
		t.Fatal(saved, ok, err)
	}
	bounds, _ := b.Bounds(w, "orders", 0)
	if bounds.Next != 3 {
		t.Fatal(bounds)
	}
	_, ok, err = db.DLQDecision(ctx, "other", "orders.DLQ", 0, 0)
	if err != nil || ok {
		t.Fatal("workspace leak", err)
	}
}
