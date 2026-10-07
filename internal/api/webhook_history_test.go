package api

import (
	"context"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
	"strings"
	"testing"
)

type webhookTestStore struct{}

func (webhookTestStore) Subscriptions(context.Context) ([]webhooks.Subscription, error) {
	return []webhooks.Subscription{{ID: "id", Workspace: "demo", Secret: "signing-secret", Headers: map[string]string{"Authorization": "Bearer hidden"}, EncryptedHeaders: "ciphertext", HeaderNames: []string{"Authorization"}}, {ID: "other-id", Workspace: "other"}}, nil
}
func (webhookTestStore) Create(context.Context, webhooks.Subscription) error { return nil }
func (webhookTestStore) Attempt(context.Context, string, string) (webhooks.Attempt, error) {
	return webhooks.Attempt{}, nil
}
func (webhookTestStore) SaveAttempt(context.Context, string, string, webhooks.Attempt) error {
	return nil
}
func (webhookTestStore) Pause(context.Context, string, string, bool) error { return nil }
func (webhookTestStore) DeliveryLogs(context.Context, string, string) ([]map[string]any, error) {
	return nil, nil
}
func (webhookTestStore) DeliveryHistory(_ context.Context, w, id string, after int64, limit int) (webhooks.HistoryPage, error) {
	return webhooks.HistoryPage{Entries: []webhooks.HistoryEntry{}, NextCursor: after}, nil
}
func TestWebhookHistoryQueriesAndSecretRedaction(t *testing.T) {
	store := webhookTestStore{}
	s := &Server{Auth: testAuth{}, Webhooks: &webhooks.Worker{Store: store}, WebhookAdmin: store}
	h := s.Handler()
	w := request(h, "GET", "/v1/webhooks", "admin", "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "hidden") || strings.Contains(w.Body.String(), "signing-secret") || strings.Contains(w.Body.String(), "ciphertext") || strings.Contains(w.Body.String(), "other-id") || !strings.Contains(w.Body.String(), "Authorization") {
		t.Fatal(w.Code, w.Body)
	}
	for _, q := range []string{"after=-1", "after=bad", "limit=0", "limit=101", "limit=bad"} {
		w = request(h, "GET", "/v1/webhooks/id/history?"+q, "admin", "")
		if w.Code != 400 {
			t.Fatal(q, w.Code, w.Body)
		}
	}
	w = request(h, "GET", "/v1/webhooks/id/history?after=9&limit=2", "admin", "")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"next_cursor":9`) {
		t.Fatal(w.Code, w.Body)
	}
	w = request(h, "GET", "/v1/webhooks/id/history", "read", "")
	if w.Code != 403 {
		t.Fatal(w.Code, w.Body)
	}
}
