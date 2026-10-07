package api

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
	"math"
	"strings"
	"testing"
	"time"
)

type metricsStore struct{ fail bool }

func (s metricsStore) WebhookMetrics(context.Context, string) (webhooks.Metrics, error) {
	if s.fail {
		return webhooks.Metrics{}, errors.New("database offline")
	}
	return webhooks.Metrics{Delivered: 4, Failed: 2, Unknown: 1, DeadLetters: 1}, nil
}
func TestMetricsAreScopedTruthfulAndFailClosed(t *testing.T) {
	b, err := storage.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for _, w := range []string{"demo", "other"} {
		b.Create(storage.Topic{Workspace: w, Name: "orders", Partitions: 1})
		for i := 0; i < 3; i++ {
			if _, err = b.Publish(w, "orders", storage.Input{Type: "order", Data: json.RawMessage(`{}`)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	g, err := groups.New(b, &testStore{}, time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	g.Create("demo", "orders", "billing", "earliest")
	s := &Server{Broker: b, Groups: g, Auth: testAuth{}, MetadataMetrics: metricsStore{}}
	h := s.Handler()
	w := request(h, "GET", "/metrics?workspace=other", "metrics", "")
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(body, "events_published_total 3\n") || strings.Contains(body, `workspace="other"`) || !strings.Contains(body, `consumer_lag{workspace="demo",topic="orders",group="billing",partition="0"} 3`) || !strings.Contains(body, `webhook_delivery_total{outcome="success"} 4`) || !strings.Contains(body, "publish_latency_seconds_count 3\n") {
		t.Fatal(w.Code, body)
	}
	snap, _ := g.Join("demo", "orders", "billing", "a")
	batch, err := g.Pull("demo", "orders", "billing", "a", snap.Epoch, 2)
	if err != nil {
		t.Fatal(err)
	}
	g.Commit("demo", "orders", "billing", "a", snap.Epoch, 0, batch[0].Token, 1)
	w = request(h, "GET", "/metrics", "metrics", "")
	if !strings.Contains(w.Body.String(), "events_consumed_total 2\n") || !strings.Contains(w.Body.String(), `consumer_lag{workspace="demo",topic="orders",group="billing",partition="0"} 2`) {
		t.Fatal(w.Body)
	}
	for _, token := range []string{"read", "produce", "consume"} {
		w = request(h, "GET", "/metrics", token, "")
		if w.Code != 403 {
			t.Fatal(token, w.Code)
		}
	}
	w = request(h, "GET", "/v1/topics", "metrics", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "orders") {
		t.Fatal("metrics key read topics", w.Code, w.Body)
	}
	w = request(h, "POST", "/v1/topics/orders/events", "metrics", `{"type":"order","data":{}}`)
	if w.Code != 403 {
		t.Fatal("metrics key produced", w.Code)
	}
	b.MinFreeBytes = math.MaxInt64
	w = request(h, "GET", "/metrics", "metrics", "")
	if !strings.Contains(w.Body.String(), "broker_storage_pressure 1\n") {
		t.Fatal("pressure threshold overflow", w.Body)
	}
	s.MetadataMetrics = metricsStore{fail: true}
	w = request(h, "GET", "/metrics", "metrics", "")
	if w.Code != 503 || strings.Contains(w.Body.String(), "events_published_total") {
		t.Fatal("partial successful scrape", w.Code, w.Body)
	}
	s.MetadataMetrics = metricsStore{}
	b.Close()
	w = request(h, "GET", "/metrics", "metrics", "")
	if w.Code != 503 {
		t.Fatal(w.Code, w.Body)
	}
}
