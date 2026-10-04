package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type testAuth struct{}

func (testAuth) Authenticate(_ context.Context, token string, cookie bool) (metadata.Principal, error) {
	p := metadata.Principal{ID: token, Workspace: "demo", Cookie: cookie, CSRF: "csrf"}
	switch token {
	case "admin":
		p.Role = "owner"
	case "produce":
		p.Scopes = []string{"topic:orders:produce"}
	case "other":
		p.Role = "owner"
		p.Workspace = "other"
	default:
		return p, errors.New("unauthorized")
	}
	return p, nil
}
func (testAuth) Login(context.Context, string, string, string) (string, string, error) {
	return "admin", "csrf", nil
}
func (testAuth) Logout(context.Context, string) error { return nil }
func (testAuth) CreateKey(context.Context, metadata.Principal, string, []string, time.Time) (string, string, error) {
	return "key", "token", nil
}
func (testAuth) RevokeKey(context.Context, string, string) error                         { return nil }
func (testAuth) Audit(context.Context, metadata.Principal, string, string, string) error { return nil }

type testStore struct{ states []groups.State }

func (s *testStore) Load() ([]groups.State, error) { return s.states, nil }
func (s *testStore) Save(v groups.State) error     { s.states = append(s.states, v); return nil }
func apiFixture(t *testing.T) (http.Handler, *storage.Broker) {
	b, err := storage.Open(t.TempDir(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	b.Create(storage.Topic{Workspace: "demo", Name: "orders", Partitions: 4})
	g, err := groups.New(b, &testStore{}, time.Minute, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return (&Server{Broker: b, Groups: g, Auth: testAuth{}, Ready: func(context.Context) error { return nil }}).Handler(), b
}
func request(h http.Handler, method, path, token, data string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, bytes.NewBufferString(data))
	r.RemoteAddr = "127.0.0.1:2000"
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestAuthorizationAndTenantIsolation(t *testing.T) {
	h, _ := apiFixture(t)
	cases := []struct {
		method, path, token, data string
		status                    int
	}{{"GET", "/v1/topics/orders", "", "", 401}, {"GET", "/v1/topics/orders", "produce", "", 403}, {"GET", "/v1/topics/orders", "other", "", 404}, {"POST", "/v1/topics/orders/events", "produce", `{"type":"order","data":{}}`, 201}, {"POST", "/v1/topics/orders/events", "admin", `{"type":"order","data":{},"workspace":"other"}`, 400}, {"POST", "/v1/topics/orders/groups", "produce", `{"name":"g","start":"earliest"}`, 403}, {"GET", "/v1/topics/orders/events?partition=0&offset=-1", "admin", "", 416}}
	for _, c := range cases {
		w := request(h, c.method, c.path, c.token, c.data)
		if w.Code != c.status {
			t.Fatalf("%s %s got %d: %s", c.method, c.path, w.Code, w.Body)
		}
	}
}
func TestCookieCSRFAndPublishConsumeAck(t *testing.T) {
	h, _ := apiFixture(t)
	r := httptest.NewRequest("POST", "/v1/topics/orders/events", bytes.NewBufferString(`{"type":"order","data":{}}`))
	r.AddCookie(&http.Cookie{Name: "eventcore_session", Value: "admin"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
	r = httptest.NewRequest("POST", "/v1/topics/orders/events", bytes.NewBufferString(`{"type":"order","key":"same","data":{}}`))
	r.AddCookie(&http.Cookie{Name: "eventcore_session", Value: "admin"})
	r.Header.Set("X-CSRF-Token", "csrf")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body)
	}
	if w = request(h, "POST", "/v1/topics/orders/groups", "admin", `{"name":"billing","start":"earliest"}`); w.Code != 201 {
		t.Fatal(w.Code, w.Body)
	}
	w = request(h, "POST", "/v1/topics/orders/groups/billing/join", "admin", `{"member":"a"}`)
	var state groups.Snapshot
	json.Unmarshal(w.Body.Bytes(), &state)
	data, _ := json.Marshal(map[string]any{"member": "a", "epoch": state.Epoch, "limit": 10})
	w = request(h, "POST", "/v1/topics/orders/groups/billing/pull", "admin", string(data))
	var deliveries []groups.Delivery
	if err := json.Unmarshal(w.Body.Bytes(), &deliveries); err != nil || len(deliveries) != 1 {
		t.Fatal(w.Code, w.Body, err)
	}
	d := deliveries[0]
	data, _ = json.Marshal(map[string]any{"member": "a", "epoch": d.Epoch, "partition": d.Partition, "token": d.Token})
	w = request(h, "POST", "/v1/topics/orders/groups/billing/ack", "admin", string(data))
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	w = request(h, "GET", "/v1/topics/orders/groups/billing", "admin", "")
	json.Unmarshal(w.Body.Bytes(), &state)
	if state.Lag[d.Partition] != 0 {
		t.Fatal(state)
	}
}
func TestBatchPartialResults(t *testing.T) {
	h, b := apiFixture(t)
	w := request(h, "POST", "/v1/topics/orders/events/batch", "produce", `{"events":[{"type":"order","data":{}},{"type":"","data":{}}]}`)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body)
	}
	var response struct{ Results []map[string]any }
	json.Unmarshal(w.Body.Bytes(), &response)
	if len(response.Results) != 2 || response.Results[0]["event"] == nil || response.Results[1]["error"] == nil {
		t.Fatal(response)
	}
	bounds, _ := b.Bounds("demo", "orders", 0)
	if bounds.Next != 1 {
		t.Fatal(bounds)
	}
}
func TestReadinessDoesNotHideDBFailure(t *testing.T) {
	s := Server{Ready: func(context.Context) error { return errors.New("db offline") }}
	w := request(s.Handler(), "GET", "/ready", "", "")
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}
