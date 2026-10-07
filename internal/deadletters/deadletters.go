// Package deadletters resolves immutable DLQ records through a durable outbox.
package deadletters

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrConflict = errors.New("DLQ record already has a different resolution")
var ErrInvalid = errors.New("record is not a valid webhook dead letter")
var ErrStore = errors.New("DLQ resolution metadata unavailable")

type Receipt struct {
	ID        string    `json:"id"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}
type Decision struct {
	Workspace   string         `json:"workspace"`
	Topic       string         `json:"topic"`
	Partition   int            `json:"partition"`
	Offset      int64          `json:"offset"`
	Actor       string         `json:"actor"`
	Action      string         `json:"action"`
	Status      string         `json:"status"`
	CreatedAt   time.Time      `json:"created_at"`
	CompletedAt *time.Time     `json:"completed_at,omitempty"`
	Receipt     *Receipt       `json:"receipt,omitempty"`
	Candidate   *storage.Event `json:"candidate,omitempty"`
}
type Store interface {
	DLQDecision(context.Context, string, string, int, int64) (Decision, bool, error)
	SaveDLQDecision(context.Context, Decision) error
	NextPendingDLQ(context.Context) (Decision, bool, error)
}
type Manager struct {
	mu     sync.Mutex
	Broker *storage.Broker
	Store  Store
}
type Failure struct {
	OriginalEvent     storage.Event `json:"original_event"`
	OriginalTopic     string        `json:"original_topic"`
	OriginalPartition int           `json:"original_partition"`
	OriginalOffset    int64         `json:"original_offset"`
	SubscriptionID    string        `json:"subscription_id"`
	Attempts          int           `json:"attempts"`
	Error             string        `json:"error"`
	FailedAt          time.Time     `json:"failed_at"`
}

func Parse(topic string, e storage.Event) (Failure, error) {
	var f Failure
	if !strings.HasSuffix(topic, ".DLQ") || e.Topic != topic || e.Type != "eventcore.delivery.failed" || json.Unmarshal(e.Data, &f) != nil || f.OriginalTopic != strings.TrimSuffix(topic, ".DLQ") || f.OriginalEvent.Topic != f.OriginalTopic || f.OriginalEvent.Partition != f.OriginalPartition || f.OriginalEvent.Offset != f.OriginalOffset || !storage.ValidName(f.OriginalEvent.ID) || f.OriginalPartition < 0 || f.OriginalOffset < 0 || f.OriginalEvent.Type == "" || !json.Valid(f.OriginalEvent.Data) || f.Attempts < 1 || f.SubscriptionID == "" {
		return f, ErrInvalid
	}
	return f, nil
}
func (m *Manager) finish(ctx context.Context, d Decision, e storage.Event) (Decision, error) {
	now := time.Now().UTC()
	d.Status = "complete"
	d.CompletedAt = &now
	d.Candidate = nil
	d.Receipt = &Receipt{ID: e.ID, Topic: e.Topic, Partition: e.Partition, Offset: e.Offset, Timestamp: e.Timestamp}
	if err := m.Store.SaveDLQDecision(ctx, d); err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrStore, err)
	}
	if err := m.Broker.CompletePrepared(d.Workspace, e.Topic, e.ID, e.Partition); err != nil {
		return Decision{}, err
	}
	return d, nil
}
func (m *Manager) resume(ctx context.Context, d Decision) (Decision, error) {
	if d.Status != "pending" || d.Action != "retry" || d.Candidate == nil {
		return Decision{}, errors.New("invalid pending DLQ outbox")
	}
	e, err := m.Broker.RestorePrepared(d.Workspace, *d.Candidate)
	if err != nil {
		return Decision{}, err
	}
	return m.finish(ctx, d, e)
}

// Recover must run before HTTP serving, webhook delivery or retention begins.
// Read one outbox row at a time rather than holding pending payloads in memory.
func (m *Manager) Recover(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for {
		d, ok, err := m.Store.NextPendingDLQ(ctx)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrStore, err)
		}
		if !ok {
			return nil
		}
		if _, err = m.resume(ctx, d); err != nil {
			return err
		}
	}
}
func (m *Manager) Resolve(ctx context.Context, w, topic, actor string, p int, offset int64, action string) (Decision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if action != "retry" && action != "discard" || p < 0 || offset < 0 {
		return Decision{}, errors.New("invalid DLQ action or cursor")
	}
	previous, ok, err := m.Store.DLQDecision(ctx, w, topic, p, offset)
	if err != nil {
		return Decision{}, fmt.Errorf("%w: %v", ErrStore, err)
	}
	if ok {
		if previous.Action != action {
			return Decision{}, ErrConflict
		}
		if previous.Status == "complete" {
			return previous, nil
		}
		return m.resume(ctx, previous)
	}
	records, err := m.Broker.Read(w, topic, p, offset, 1)
	if err != nil {
		return Decision{}, err
	}
	if len(records) != 1 {
		return Decision{}, storage.ErrRange
	}
	original, err := Parse(topic, records[0])
	if err != nil {
		return Decision{}, err
	}
	d := Decision{Workspace: w, Topic: topic, Partition: p, Offset: offset, Actor: actor, Action: action, CreatedAt: time.Now().UTC()}
	if action == "discard" {
		d.Status = "complete"
		now := time.Now().UTC()
		d.CompletedAt = &now
		if err = m.Store.SaveDLQDecision(ctx, d); err != nil {
			return Decision{}, fmt.Errorf("%w: %v", ErrStore, err)
		}
		return d, nil
	}
	headers := map[string]string{}
	for k, v := range original.OriginalEvent.Headers {
		headers[k] = v
	}
	headers["eventcore.dlq.source_id"] = records[0].ID
	headers["eventcore.dlq.original_id"] = original.OriginalEvent.ID
	headers["eventcore.dlq.source_offset"] = strconv.FormatInt(offset, 10)
	headers["eventcore.dlq.source_partition"] = strconv.Itoa(p)
	input := storage.Input{Type: original.OriginalEvent.Type, Key: original.OriginalEvent.Key, Data: original.OriginalEvent.Data, Headers: headers}
	e, err := m.Broker.PublishPrepared(w, original.OriginalTopic, input, func(e storage.Event) error {
		d.Status = "pending"
		d.Candidate = &e
		return m.Store.SaveDLQDecision(ctx, d)
	})
	if err != nil {
		return Decision{}, err
	}
	return m.finish(ctx, d, e)
}
