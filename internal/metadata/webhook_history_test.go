package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
	"os"
	"strings"
	"testing"
	"time"
)

func TestPostgresWebhookHistoryAndEncryption(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.SQL.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	w := "history-" + storage.ID()
	if _, err = db.SQL.ExecContext(ctx, `INSERT INTO workspaces(id) VALUES($1)`, w); err != nil {
		t.Fatal(err)
	}
	id := storage.ID()
	key := bytes.Repeat([]byte{2}, 32)
	headers, err := webhooks.EncryptHeaders(key, map[string]string{"Authorization": "Bearer confidential"})
	if err != nil {
		t.Fatal(err)
	}
	if err = db.Create(ctx, webhooks.Subscription{ID: id, Workspace: w, Topic: "orders", URL: "https://example.com", EncryptedSecret: "encrypted-test", EncryptedHeaders: headers, HeaderNames: []string{"Authorization"}, MaxAttempts: 3, DelaySeconds: 1}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.SQL.ExecContext(ctx, `DELETE FROM webhook_delivery_history WHERE subscription_id=$1`, id)
		db.SQL.ExecContext(ctx, `DELETE FROM webhook_attempts WHERE subscription_id=$1`, id)
		db.SQL.ExecContext(ctx, `DELETE FROM webhooks WHERE id=$1`, id)
		db.SQL.ExecContext(ctx, `DELETE FROM workspaces WHERE id=$1`, w)
	}()
	subs, err := db.Subscriptions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range subs {
		if s.ID == id {
			found = true
			if s.Headers != nil || s.EncryptedHeaders != headers || len(s.HeaderNames) != 1 {
				t.Fatal(s)
			}
		}
	}
	if !found {
		t.Fatal("subscription not restored")
	}
	start := time.Now().UTC()
	done := start.Add(time.Millisecond)
	a := webhooks.Attempt{Count: 1, Status: "sending", StartedAt: start}
	if err = db.SaveAttempt(ctx, id, "event", a); err != nil {
		t.Fatal(err)
	}
	a.Status = "retry"
	a.HTTPStatus = 500
	a.CompletedAt = &done
	a.Error = "HTTP 500"
	if err = db.SaveAttempt(ctx, id, "event", a); err != nil {
		t.Fatal(err)
	}
	if err = db.SaveAttempt(ctx, id, "event", a); err != nil {
		t.Fatal(err)
	}
	a.Count = 2
	a.Status = "sending"
	a.HTTPStatus = 0
	a.CompletedAt = nil
	a.Error = ""
	if err = db.SaveAttempt(ctx, id, "event", a); err != nil {
		t.Fatal(err)
	}
	a.Status = "delivered"
	a.HTTPStatus = 200
	a.CompletedAt = &done
	if err = db.SaveAttempt(ctx, id, "event", a); err != nil {
		t.Fatal(err)
	}
	page, err := db.DeliveryHistory(ctx, w, id, 0, 2)
	if err != nil || len(page.Entries) != 2 || !page.HasMore || page.Entries[0].Attempt.Status != "sending" || page.Entries[1].Attempt.HTTPStatus != 500 {
		t.Fatal(page, err)
	}
	tail, err := db.DeliveryHistory(ctx, w, id, page.NextCursor, 2)
	if err != nil || len(tail.Entries) != 2 || tail.HasMore || tail.Entries[1].Attempt.Status != "delivered" {
		t.Fatal(tail, err)
	}
	raw, _ := json.Marshal(page)
	if bytes.Contains(raw, []byte("confidential")) {
		t.Fatal("secret leak")
	}
	other, err := db.DeliveryHistory(ctx, "other", id, 0, 100)
	if err != nil || len(other.Entries) != 0 {
		t.Fatal("workspace isolation", other, err)
	}
	// Legacy latest-only records are backfilled once, without inventing older attempts.
	legacy, _ := json.Marshal(webhooks.Attempt{Count: 3, Status: "dlq", HTTPStatus: 500})
	if _, err = db.SQL.ExecContext(ctx, `INSERT INTO webhook_attempts(subscription_id,event_id,attempt) VALUES($1,'legacy',$2)`, id, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err = db.SQL.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version='webhook_history_v1'`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = db.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	page, err = db.DeliveryHistory(ctx, w, id, 0, 100)
	if err != nil || len(page.Entries) != 5 {
		t.Fatal("backfill", page, err)
	}
	// Reject a history write after updating latest state: both writes must roll back.
	fn := "reject_" + strings.ReplaceAll(storage.ID(), "-", "")
	ddl := `CREATE FUNCTION ` + fn + `() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.subscription_id='` + id + `' AND NEW.event_id='rollback' THEN RAISE EXCEPTION 'test history failure'; END IF; RETURN NEW; END $$`
	if _, err = db.SQL.ExecContext(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	if _, err = db.SQL.ExecContext(ctx, `CREATE TRIGGER `+fn+` BEFORE INSERT ON webhook_delivery_history FOR EACH ROW EXECUTE FUNCTION `+fn+`() `); err != nil {
		t.Fatal(err)
	}
	defer func() {
		db.SQL.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+fn+` ON webhook_delivery_history`)
		db.SQL.ExecContext(ctx, `DROP FUNCTION IF EXISTS `+fn+`() `)
	}()
	if err = db.SaveAttempt(ctx, id, "rollback", webhooks.Attempt{Count: 1, Status: "sending"}); err == nil {
		t.Fatal("failed history accepted")
	}
	state, err := db.Attempt(ctx, id, "rollback")
	if err != nil || state.Count != 0 {
		t.Fatal("latest state was not rolled back", state, err)
	}
}
