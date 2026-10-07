package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/The-Null-Catchers/EventCore/internal/deadletters"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Auth interface {
	Authenticate(context.Context, string, bool) (metadata.Principal, error)
	Login(context.Context, string, string, string) (string, string, error)
	Logout(context.Context, string) error
	CreateKey(context.Context, metadata.Principal, string, []string, time.Time) (string, string, error)
	RevokeKey(context.Context, string, string) error
	Audit(context.Context, metadata.Principal, string, string, string) error
}
type WebhookAdmin interface {
	Pause(context.Context, string, string, bool) error
	DeliveryLogs(context.Context, string, string) ([]map[string]any, error)
}
type bucket struct {
	start time.Time
	count int
}
type Server struct {
	DeadLetters  *deadletters.Manager
	streams      map[string]int
	Webhooks     *webhooks.Worker
	WebhookAdmin WebhookAdmin

	Broker              *storage.Broker
	Groups              *groups.Coordinator
	Auth                Auth
	Ready               func(context.Context) error
	SecureCookies       bool
	Published, Consumed atomic.Uint64
	mu                  sync.Mutex
	limits              map[string]bucket
	started             time.Time
	MaxRPS              int
}

func (s *Server) allow(k string, limit int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.limits == nil {
		s.limits = map[string]bucket{}
	}
	v := s.limits[k]
	if now.Sub(v.start) >= time.Second {
		v = bucket{start: now}
	}
	if v.count >= limit {
		return false
	}
	if len(s.limits) >= 10000 {
		for key, b := range s.limits {
			if now.Sub(b.start) > time.Minute {
				delete(s.limits, key)
			}
		}
		if _, ok := s.limits[k]; !ok && len(s.limits) >= 10000 {
			return false
		}
	}
	v.count++
	s.limits[k] = v
	return true
}
func Allowed(p metadata.Principal, topic, action string) bool {
	if p.Role == "owner" || p.Role == "admin" {
		return true
	}
	if p.Role == "developer" {
		return action != "admin"
	}
	if p.Role == "viewer" {
		return action == "read"
	}
	for _, scope := range p.Scopes {
		if scope == "admin" || scope == "topic:"+topic+":"+action || scope == "topic:*:"+action {
			return true
		}
	}
	return false
}
func reply(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}
func fail(w http.ResponseWriter, status int, err error) {
	reply(w, status, map[string]string{"error": err.Error()})
}
func body(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return errors.New("expected one JSON value")
	}
	return nil
}
func (s *Server) principal(r *http.Request) (metadata.Principal, error) {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		return s.Auth.Authenticate(r.Context(), strings.TrimPrefix(h, "Bearer "), false)
	}
	cookie, err := r.Cookie("eventcore_session")
	if err != nil {
		return metadata.Principal{}, err
	}
	p, err := s.Auth.Authenticate(r.Context(), cookie.Value, true)
	if err == nil && r.Method != "GET" && r.Method != "HEAD" {
		if subtle.ConstantTimeCompare([]byte(p.CSRF), []byte(r.Header.Get("X-CSRF-Token"))) != 1 {
			return p, errors.New("CSRF token required")
		}
	}
	return p, err
}
func (s *Server) Handler() http.Handler { s.started = time.Now(); return http.HandlerFunc(s.serve) }
func (s *Server) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Frame-Options", "DENY")
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if !s.allow("ip:"+ip, 100) {
		w.Header().Set("Retry-After", "1")
		fail(w, 429, errors.New("IP request limit"))
		return
	}
	if r.URL.Path == "/health" && r.Method == "GET" {
		reply(w, 200, map[string]string{"status": "running", "version": "0.1.0-dev"})
		return
	}
	if r.URL.Path == "/ready" && r.Method == "GET" {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := s.Ready(ctx); err != nil {
			slog.Error("readiness failed", "error", err)
			fail(w, 503, errors.New("essential infrastructure unavailable"))
			return
		}
		reply(w, 200, map[string]string{"status": "ready"})
		return
	}
	if r.URL.Path == "/v1/auth/login" && r.Method == "POST" {
		if !s.allow("login:"+ip, 2) {
			fail(w, 429, errors.New("login rate limit"))
			return
		}
		var req struct {
			Email     string `json:"email"`
			Password  string `json:"password"`
			Workspace string `json:"workspace"`
		}
		if err := body(w, r, &req); err != nil {
			fail(w, 400, err)
			return
		}
		if len(req.Email) > 320 || len(req.Password) > 72 {
			fail(w, 400, errors.New("invalid credentials"))
			return
		}
		token, csrf, err := s.Auth.Login(r.Context(), req.Email, req.Password, req.Workspace)
		if err != nil {
			fail(w, 401, errors.New("invalid credentials"))
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "eventcore_session", Value: token, Path: "/", HttpOnly: true, Secure: s.SecureCookies, SameSite: http.SameSiteStrictMode, MaxAge: 43200})
		reply(w, 200, map[string]string{"csrf_token": csrf})
		return
	}
	p, err := s.principal(r)
	if err != nil {
		fail(w, 401, errors.New("authentication or CSRF validation failed"))
		return
	}
	max := s.MaxRPS
	if max <= 0 {
		max = 200
	}
	if !s.allow("key:"+p.ID, max) || !s.allow("workspace:"+p.Workspace, max*2) {
		w.Header().Set("Retry-After", "1")
		fail(w, 429, errors.New("workspace or API key request limit"))
		return
	}
	if r.URL.Path == "/v1/auth/me" && r.Method == "GET" {
		reply(w, 200, map[string]any{"id": p.ID, "workspace": p.Workspace, "role": p.Role, "csrf_token": p.CSRF})
		return
	}
	if r.URL.Path == "/v1/auth/logout" && r.Method == "POST" {
		cookie, err := r.Cookie("eventcore_session")
		if err == nil {
			err = s.Auth.Logout(r.Context(), cookie.Value)
		}
		if err != nil {
			fail(w, 400, err)
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "eventcore_session", Value: "", Path: "/", HttpOnly: true, Secure: s.SecureCookies, SameSite: http.SameSiteStrictMode, MaxAge: -1})
		reply(w, 200, map[string]bool{"ok": true})
		return
	}
	if r.URL.Path == "/metrics" && r.Method == "GET" {
		if !Allowed(p, "*", "admin") {
			fail(w, 403, errors.New("admin scope required"))
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# TYPE events_published_total counter\nevents_published_total %d\n# TYPE events_consumed_total counter\nevents_consumed_total %d\n# TYPE broker_uptime_seconds gauge\nbroker_uptime_seconds %.3f\n", s.Published.Load(), s.Consumed.Load(), time.Since(s.started).Seconds())
		for _, t := range s.Broker.Topics(p.Workspace) {
			for i := 0; i < t.Partitions; i++ {
				b, err := s.Broker.Bounds(p.Workspace, t.Name, i)
				if err != nil {
					continue
				}
				fmt.Fprintf(w, "broker_disk_bytes{workspace=%q,topic=%q,partition=%q} %d\n", p.Workspace, t.Name, strconv.Itoa(i), b.Bytes)
			}
		}
		return
	}
	if r.URL.Path == "/v1/keys" && r.Method == "POST" {
		if !Allowed(p, "*", "admin") {
			fail(w, 403, errors.New("admin scope required"))
			return
		}
		var req struct {
			Name    string    `json:"name"`
			Scopes  []string  `json:"scopes"`
			Expires time.Time `json:"expires_at"`
		}
		if err = body(w, r, &req); err != nil {
			fail(w, 400, err)
			return
		}
		if req.Name == "" || len(req.Name) > 128 || len(req.Scopes) == 0 || len(req.Scopes) > 64 || !req.Expires.After(time.Now()) || req.Expires.After(time.Now().Add(366*24*time.Hour)) {
			fail(w, 400, errors.New("name, scopes and expiry within one year required"))
			return
		}
		for _, scope := range req.Scopes {
			parts := strings.Split(scope, ":")
			if scope != "admin" && (len(parts) != 3 || parts[0] != "topic" || (!storage.ValidName(parts[1]) && parts[1] != "*") || (parts[2] != "produce" && parts[2] != "consume" && parts[2] != "read")) {
				fail(w, 400, errors.New("invalid scope"))
				return
			}
		}
		if !s.audit(w, r, p, "key.create", req.Name) {
			return
		}
		id, token, err := s.Auth.CreateKey(r.Context(), p, req.Name, req.Scopes, req.Expires)
		if err != nil {
			s.internal(w, err)
			return
		}
		reply(w, 201, map[string]string{"id": id, "token": token})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/keys/") && r.Method == "DELETE" {
		if !Allowed(p, "*", "admin") {
			fail(w, 403, errors.New("admin scope required"))
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/v1/keys/")
		if !s.audit(w, r, p, "key.revoke", id) {
			return
		}
		if err = s.Auth.RevokeKey(r.Context(), p.Workspace, id); err != nil {
			fail(w, 404, err)
			return
		}
		reply(w, 200, map[string]bool{"ok": true})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/v1/webhooks") {
		if !Allowed(p, "*", "admin") {
			fail(w, 403, errors.New("admin required"))
			return
		}
		if s.Webhooks == nil {
			fail(w, 503, errors.New("webhooks not configured"))
			return
		}
		segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(segments) == 2 && r.Method == "GET" {
			subs, err := s.Webhooks.Store.Subscriptions(r.Context())
			if err != nil {
				s.internal(w, err)
				return
			}
			out := []webhooks.Subscription{}
			for _, sub := range subs {
				if sub.Workspace == p.Workspace {
					sub.Secret = ""
					out = append(out, sub)
				}
			}
			reply(w, 200, out)
			return
		}
		if len(segments) == 2 && r.Method == "POST" {
			var sub webhooks.Subscription
			if err := body(w, r, &sub); err != nil {
				fail(w, 400, err)
				return
			}
			sub.Workspace = p.Workspace
			if !s.audit(w, r, p, "webhook.create", sub.Topic) {
				return
			}
			if err := s.Webhooks.Initialize(r.Context(), sub); err != nil {
				fail(w, 400, err)
				return
			}
			reply(w, 201, map[string]bool{"ok": true})
			return
		}
		if len(segments) == 3 && r.Method == "PATCH" {
			var req struct {
				Paused bool `json:"paused"`
			}
			if err := body(w, r, &req); err != nil {
				fail(w, 400, err)
				return
			}
			if !s.audit(w, r, p, "webhook.pause", segments[2]) {
				return
			}
			if err := s.WebhookAdmin.Pause(r.Context(), p.Workspace, segments[2], req.Paused); err != nil {
				fail(w, 400, err)
				return
			}
			reply(w, 200, map[string]bool{"ok": true})
			return
		}
		if len(segments) == 4 && segments[3] == "attempts" && r.Method == "GET" {
			logs, err := s.WebhookAdmin.DeliveryLogs(r.Context(), p.Workspace, segments[2])
			if err != nil {
				s.internal(w, err)
				return
			}
			reply(w, 200, logs)
			return
		}
		fail(w, 404, errors.New("webhook route not found"))
		return
	}
	if r.URL.Path == "/v1/topics" {
		if r.Method == "GET" {
			topics := []storage.Topic{}
			for _, t := range s.Broker.Topics(p.Workspace) {
				if Allowed(p, t.Name, "read") {
					topics = append(topics, t)
				}
			}
			reply(w, 200, topics)
			return
		}
		if r.Method == "POST" {
			if !Allowed(p, "*", "admin") {
				fail(w, 403, errors.New("admin required"))
				return
			}
			var req storage.Topic
			if err = body(w, r, &req); err != nil {
				fail(w, 400, err)
				return
			}
			req.Workspace = p.Workspace
			if strings.HasSuffix(req.Name, ".DLQ") {
				fail(w, 400, errors.New(".DLQ suffix is reserved"))
				return
			}
			if req.MaxEventBytes > 1<<20 {
				fail(w, 400, errors.New("topic event maximum is 1 MiB"))
				return
			}
			if len(s.Broker.Topics(p.Workspace)) >= 100 {
				fail(w, 409, errors.New("topic limit"))
				return
			}
			if !s.audit(w, r, p, "topic.create", req.Name) {
				return
			}
			if err = s.Broker.Create(req); err != nil {
				fail(w, 400, err)
				return
			}
			reply(w, 201, map[string]bool{"ok": true})
			return
		}
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "v1" || parts[1] != "topics" || !storage.ValidName(parts[2]) {
		fail(w, 404, errors.New("route not found"))
		return
	}
	topic := parts[2]
	action := "read"
	if len(parts) > 3 && parts[3] == "events" && r.Method == "POST" {
		action = "produce"
	}
	if len(parts) > 3 && parts[3] == "groups" {
		action = "consume"
		if len(parts) == 4 && r.Method == "POST" || len(parts) > 5 && parts[5] == "reset" {
			action = "admin"
		}
	}
	if len(parts) > 3 && parts[3] == "dead-letters" && r.Method != "GET" {
		action = "admin"
	}
	if !Allowed(p, topic, action) {
		fail(w, 403, errors.New("scope denied"))
		return
	}
	if len(parts) > 3 && parts[3] == "dead-letters" {
		s.deadLetters(w, r, p, topic, parts)
		return
	}
	if len(parts) == 4 && parts[3] == "export" && r.Method == "GET" {
		s.export(w, r, p, topic)
		return
	}
	if len(parts) == 4 && parts[3] == "replay" && r.Method == "POST" {
		s.replay(w, r, p, topic)
		return
	}
	if len(parts) == 3 && r.Method == "GET" {
		cfg, err := s.Broker.Config(p.Workspace, topic)
		if err != nil {
			fail(w, 404, err)
			return
		}
		bounds := []storage.Bounds{}
		for i := 0; i < cfg.Partitions; i++ {
			v, err := s.Broker.Bounds(p.Workspace, topic, i)
			if err != nil {
				s.internal(w, err)
				return
			}
			bounds = append(bounds, v)
		}
		reply(w, 200, map[string]any{"topic": cfg, "partitions": bounds})
		return
	}
	if len(parts) == 4 && parts[3] == "stream" && r.Method == "GET" {
		s.stream(w, r, p, topic)
		return
	}
	if len(parts) == 5 && parts[3] == "events" && parts[4] == "batch" && r.Method == "POST" {
		var req struct {
			Events []storage.Input `json:"events"`
		}
		if err = body(w, r, &req); err != nil {
			fail(w, 400, err)
			return
		}
		if len(req.Events) < 1 || len(req.Events) > 100 {
			fail(w, 400, errors.New("batch must contain 1..100 events"))
			return
		}
		results := []map[string]any{}
		for _, in := range req.Events {
			event, err := s.Broker.Publish(p.Workspace, topic, in)
			if err != nil {
				status := 400
				if errors.Is(err, storage.ErrIdempotencyConflict) {
					status = 409
				}
				if errors.Is(err, storage.ErrUnavailable) {
					status = 503
				}
				results = append(results, map[string]any{"error": err.Error(), "status": status})
			} else {
				if !event.Deduplicated {
					s.Published.Add(1)
				}
				results = append(results, map[string]any{"event": event})
			}
		}
		reply(w, 200, map[string]any{"results": results})
		return
	}
	if len(parts) == 4 && parts[3] == "events" {
		if r.Method == "POST" {
			var in storage.Input
			if err = body(w, r, &in); err != nil {
				fail(w, 400, err)
				return
			}
			event, err := s.Broker.Publish(p.Workspace, topic, in)
			if err != nil {
				if errors.Is(err, storage.ErrUnavailable) {
					s.internal(w, err)
				} else if errors.Is(err, storage.ErrIdempotencyConflict) {
					fail(w, 409, err)
				} else {
					fail(w, 400, err)
				}
				return
			}
			if !event.Deduplicated {
				s.Published.Add(1)
			}
			reply(w, 201, event)
			return
		}
		if r.Method == "GET" {
			s.events(w, r, p, topic)
			return
		}
	}
	if len(parts) >= 4 && parts[3] == "groups" {
		s.group(w, r, p, topic, parts)
		return
	}
	fail(w, 404, errors.New("route not found"))
}
func (s *Server) internal(w http.ResponseWriter, err error) {
	slog.Error("request failed", "error", err)
	fail(w, 503, errors.New("infrastructure operation failed"))
}
func (s *Server) audit(w http.ResponseWriter, r *http.Request, p metadata.Principal, action, resource string) bool {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if err := s.Auth.Audit(r.Context(), p, "attempt."+action, resource, ip); err != nil {
		s.internal(w, err)
		return false
	}
	return true
}
func (s *Server) events(w http.ResponseWriter, r *http.Request, p metadata.Principal, topic string) {
	q, err := scanQuery(r)
	if err != nil {
		fail(w, 400, err)
		return
	}
	page, err := s.Broker.Scan(p.Workspace, topic, q)
	if err != nil {
		fail(w, replayStatus(err), err)
		return
	}
	reply(w, 200, page)
}

func (s *Server) group(w http.ResponseWriter, r *http.Request, p metadata.Principal, topic string, parts []string) {
	if len(parts) == 4 && r.Method == "POST" {
		var req struct {
			Name  string `json:"name"`
			Start string `json:"start"`
		}
		if err := body(w, r, &req); err != nil {
			fail(w, 400, err)
			return
		}
		if err := s.Groups.Create(p.Workspace, topic, req.Name, req.Start); err != nil {
			fail(w, 400, err)
			return
		}
		reply(w, 201, map[string]bool{"ok": true})
		return
	}
	if len(parts) < 5 || !storage.ValidName(parts[4]) {
		fail(w, 404, errors.New("group route not found"))
		return
	}
	name := parts[4]
	if len(parts) == 5 && r.Method == "GET" {
		snap, err := s.Groups.Inspect(p.Workspace, topic, name)
		if err != nil {
			fail(w, 404, err)
			return
		}
		reply(w, 200, snap)
		return
	}
	if len(parts) != 6 || r.Method != "POST" {
		fail(w, 404, errors.New("group operation not found"))
		return
	}
	var req struct {
		Member     string `json:"member"`
		Epoch      uint64 `json:"epoch"`
		Partition  int    `json:"partition"`
		Token      string `json:"token"`
		Limit      int    `json:"limit"`
		Offset     int64  `json:"offset"`
		Confirm    bool   `json:"confirm"`
		NextOffset *int64 `json:"next_offset"`
	}
	if err := body(w, r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	var result any = map[string]bool{"ok": true}
	var err error
	switch parts[5] {
	case "join":
		result, err = s.Groups.Join(p.Workspace, topic, name, req.Member)
	case "leave":
		err = s.Groups.Leave(p.Workspace, topic, name, req.Member)
	case "pull":
		if req.Limit == 0 {
			req.Limit = 100
		}
		var deliveries []groups.Delivery
		deliveries, err = s.Groups.Pull(p.Workspace, topic, name, req.Member, req.Epoch, req.Limit)
		if err == nil {
			for _, d := range deliveries {
				s.Consumed.Add(uint64(len(d.Events)))
			}
		}
		result = deliveries
	case "ack", "nack":
		err = s.Groups.Ack(p.Workspace, topic, name, req.Member, req.Epoch, req.Partition, req.Token, parts[5] == "nack")
	case "commit":
		if req.NextOffset == nil {
			fail(w, 400, errors.New("next_offset required"))
			return
		}
		err = s.Groups.Commit(p.Workspace, topic, name, req.Member, req.Epoch, req.Partition, req.Token, *req.NextOffset)
	case "reset":
		if !req.Confirm {
			fail(w, 400, errors.New("confirm=true required"))
			return
		}
		if !s.audit(w, r, p, "offset.reset", topic+"/"+name) {
			return
		}
		err = s.Groups.Reset(p.Workspace, topic, name, req.Partition, req.Offset)
	default:
		fail(w, 404, errors.New("unknown group operation"))
		return
	}
	if err != nil {
		status := 400
		if errors.Is(err, groups.ErrFenced) {
			status = 409
		}
		if errors.Is(err, storage.ErrRange) {
			status = 416
		}
		fail(w, status, err)
		return
	}
	reply(w, 200, result)
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request, p metadata.Principal, topic string) {
	partition, err := strconv.Atoi(r.URL.Query().Get("partition"))
	if err != nil {
		fail(w, 400, errors.New("partition required"))
		return
	}
	bounds, err := s.Broker.Bounds(p.Workspace, topic, partition)
	if err != nil {
		fail(w, 404, err)
		return
	}
	offset := bounds.Next
	if raw := r.URL.Query().Get("offset"); raw != "" {
		offset, err = strconv.ParseInt(raw, 10, 64)
	}
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		offset, err = strconv.ParseInt(last, 10, 64)
		if err == nil && offset < 1<<63-1 {
			offset++
		} else {
			err = errors.New("invalid Last-Event-ID")
		}
	}
	if err != nil || offset < bounds.Oldest || offset > bounds.Next {
		fail(w, 416, storage.ErrRange)
		return
	}
	s.mu.Lock()
	if s.streams == nil {
		s.streams = map[string]int{}
	}
	total := 0
	for _, v := range s.streams {
		total += v
	}
	if total >= 64 || s.streams[p.Workspace] >= 16 {
		s.mu.Unlock()
		fail(w, 429, errors.New("stream connection limit"))
		return
	}
	s.streams[p.Workspace]++
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.streams[p.Workspace]--; s.mu.Unlock() }()
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	controller := http.NewResponseController(w)
	send := func(text string) error {
		if err := controller.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		if _, err := io.WriteString(w, text); err != nil {
			return err
		}
		return controller.Flush()
	}
	if err = send(": connected\n\n"); err != nil {
		return
	}
	lastAuthentication := time.Now()
	for {
		if time.Since(lastAuthentication) >= 30*time.Second {
			updated, err := s.principal(r)
			if err != nil || updated.Workspace != p.Workspace || !Allowed(updated, topic, "read") {
				return
			}
			lastAuthentication = time.Now()
		}
		events, err := s.Broker.Read(p.Workspace, topic, partition, offset, 100)
		if err != nil {
			send("event: error\ndata: {\"error\":\"retained range unavailable\"}\n\n")
			return
		}
		for _, event := range events {
			offset = event.Offset + 1
			if kind := r.URL.Query().Get("type"); kind != "" && kind != event.Type {
				continue
			}
			raw, err := json.Marshal(event)
			if err != nil {
				return
			}
			if err = send(fmt.Sprintf("id: %d\ndata: %s\n\n", event.Offset, raw)); err != nil {
				return
			}
		}
		if len(events) > 0 {
			continue
		}
		waitCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		err = s.Broker.Wait(waitCtx, p.Workspace, topic, partition, offset)
		cancel()
		if r.Context().Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			if err = send(": heartbeat\n\n"); err != nil {
				return
			}
		}
	}
}
