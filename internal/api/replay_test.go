package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"net/url"
	"strings"
	"testing"

	"github.com/The-Null-Catchers/EventCore/internal/storage"
)

func TestReplayCopyExportAuthorizationAndFailure(t *testing.T) {
	h, b := apiFixture(t)
	if err := b.Create(storage.Topic{Workspace: "demo", Name: "archive", Partitions: 2}); err != nil {
		t.Fatal(err)
	}
	var originals []storage.Event
	for i := 0; i < 4; i++ {
		e, err := b.Publish("demo", "orders", storage.Input{Type: "order", Key: "same", Data: json.RawMessage(`{"value":1}`)})
		if err != nil {
			t.Fatal(err)
		}
		originals = append(originals, e)
	}
	partition := originals[0].Partition
	path := "/v1/topics/orders/replay"
	body := map[string]any{"partition": partition, "offset": 0, "end_offset": 4, "target": "archive", "replay_id": "run-1", "confirm": true, "limit": 2}
	raw, _ := json.Marshal(body)
	for _, token := range []string{"produce", "read"} {
		w := request(h, "POST", path, token, string(raw))
		if w.Code != 403 {
			t.Fatal(token, w.Code, w.Body)
		}
	}
	w := request(h, "POST", path, "copy", string(raw))
	var result replayResult
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != 200 || len(result.Receipts) != 2 || result.NextOffset != 2 || result.Done {
		t.Fatal(w.Code, w.Body, err)
	}
	originalReceipt := result.Receipts[0].Event
	w = request(h, "POST", path, "copy", string(raw))
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 200 || !result.Receipts[0].Event.Deduplicated || result.Receipts[0].Event.ID != originalReceipt.ID {
		t.Fatal(w.Code, w.Body)
	}
	body["offset"] = 2
	raw, _ = json.Marshal(body)
	w = request(h, "POST", path, "copy", string(raw))
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 200 || !result.Done || result.NextOffset != 4 {
		t.Fatal(w.Code, w.Body)
	}
	for _, r := range result.Receipts {
		if r.Event.Headers["eventcore.replay.source_topic"] != "orders" || r.Event.Timestamp.Equal(originals[2].Timestamp) || r.Event.ID == originals[2].ID {
			t.Fatal(r)
		}
	}
	before, _ := b.Bounds("demo", "orders", partition)
	if before.Next != 4 {
		t.Fatal("source changed", before)
	}
	query := url.Values{"partition": {jsonNumber(partition)}, "offset": {"0"}, "end_offset": {"4"}, "limit": {"4"}, "from_time": {originals[1].Timestamp.Format("2006-01-02T15:04:05.999999999Z07:00")}, "until_time": {originals[3].Timestamp.Format("2006-01-02T15:04:05.999999999Z07:00")}}
	w = request(h, "GET", "/v1/topics/orders/export?"+query.Encode(), "read", "")
	lines := strings.Split(strings.TrimSpace(w.Body.String()), "\n")
	if w.Code != 200 || len(lines) != 2 || w.Header().Get("X-EventCore-Next-Offset") != "4" || w.Header().Get("X-EventCore-Done") != "true" {
		t.Fatal(w.Code, w.Body, w.Header())
	}
	var exported storage.Event
	json.Unmarshal([]byte(lines[0]), &exported)
	if exported.ID != originals[1].ID {
		t.Fatal(exported)
	}
	query.Set("from_time", "invalid")
	if w = request(h, "GET", "/v1/topics/orders/events?"+query.Encode(), "read", ""); w.Code != 400 {
		t.Fatal(w.Code)
	}
	// Failure after one successful append returns a durable prefix and failed cursor.
	b.Create(storage.Topic{Workspace: "demo", Name: "strict", Partitions: 1, Schema: json.RawMessage(`{"type":"object","properties":{"value":{"const":1}}}`)})
	e, _ := b.Publish("demo", "orders", storage.Input{Type: "order", Key: "same", Data: json.RawMessage(`{"value":2}`)})
	body["target"] = "strict"
	body["offset"] = 3
	body["end_offset"] = e.Offset + 1
	raw, _ = json.Marshal(body)
	w = request(h, "POST", path, "admin", string(raw))
	json.Unmarshal(w.Body.Bytes(), &result)
	if w.Code != 400 || len(result.Receipts) != 1 || result.NextOffset != 4 || result.Error == "" || result.Done {
		t.Fatal(w.Code, w.Body)
	}
	// Retrying the same run does not duplicate the successfully copied prefix.
	w = request(h, "POST", path, "admin", string(raw))
	bounds, _ := b.Bounds("demo", "strict", 0)
	if w.Code != 400 || bounds.Next != 1 {
		t.Fatal(w.Code, w.Body, bounds)
	}
	body["target"] = "orders"
	raw, _ = json.Marshal(body)
	if w = request(h, "POST", path, "admin", string(raw)); w.Code != 400 {
		t.Fatal("self replay accepted")
	}
}
func jsonNumber(v int) string { raw, _ := json.Marshal(v); return string(raw) }

type unavailableAudit struct{ testAuth }

func (unavailableAudit) Audit(context.Context, metadata.Principal, string, string, string) error {
	return errors.New("audit database unavailable")
}
func TestReplayAuditFailurePreventsWrites(t *testing.T) {
	_, b := apiFixture(t)
	b.Create(storage.Topic{Workspace: "demo", Name: "archive", Partitions: 1})
	e, _ := b.Publish("demo", "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)})
	h := (&Server{Broker: b, Auth: unavailableAudit{}}).Handler()
	raw, _ := json.Marshal(map[string]any{"partition": e.Partition, "offset": 0, "end_offset": 1, "target": "archive", "replay_id": "audit", "confirm": true})
	w := request(h, "POST", "/v1/topics/orders/replay", "admin", string(raw))
	bounds, _ := b.Bounds("demo", "archive", 0)
	if w.Code != 503 || bounds.Next != 0 {
		t.Fatal(w.Code, w.Body, bounds)
	}
}
