package storage

import (
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"time"
)

// Deduplication is bounded by age, retained records and index capacity per topic.
const IdempotencyCapacity = 4096
const IdempotencyWindow = 24 * time.Hour

var ErrIdempotencyConflict = errors.New("idempotency key already used for a different event")

type receipt struct {
	key       string
	hash      [32]byte
	partition int
	offset    int64
	timestamp time.Time
	id        string
	index     int
}
type receiptHeap []*receipt

func (h receiptHeap) Len() int { return len(h) }
func (h receiptHeap) Less(i, j int) bool {
	if h[i].timestamp.Equal(h[j].timestamp) {
		return h[i].id < h[j].id
	}
	return h[i].timestamp.Before(h[j].timestamp)
}
func (h receiptHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *receiptHeap) Push(v any)   { r := v.(*receipt); r.index = len(*h); *h = append(*h, r) }
func (h *receiptHeap) Pop() any {
	old := *h
	r := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	return r
}

type dedupIndex struct {
	byKey  map[string]*receipt
	oldest receiptHeap
}

func (d *dedupIndex) remove(r *receipt) { heap.Remove(&d.oldest, r.index); delete(d.byKey, r.key) }
func (d *dedupIndex) prune(now time.Time) {
	for len(d.oldest) > 0 && !d.oldest[0].timestamp.Add(IdempotencyWindow).After(now) {
		d.remove(d.oldest[0])
	}
}

// fingerprint preserves JSON numbers without float64 rounding, normalizes object
// order/whitespace, and excludes the idempotency key and server-assigned fields.
func fingerprint(in Input) ([32]byte, error) {
	var data any
	decoder := json.NewDecoder(bytes.NewReader(in.Data))
	decoder.UseNumber()
	if err := decoder.Decode(&data); err != nil {
		return [32]byte{}, err
	}
	headers := in.Headers
	if headers == nil {
		headers = map[string]string{}
	}
	raw, err := json.Marshal([]any{in.Type, in.Key, headers, data})
	return sha256.Sum256(raw), err
}
func (d *dedupIndex) remember(e Event, now time.Time) error {
	if e.IdempotencyKey == "" || !e.Timestamp.Add(IdempotencyWindow).After(now) {
		return nil
	}
	hash, err := fingerprint(e.Input)
	if err != nil {
		return err
	}
	if d.byKey == nil {
		d.byKey = map[string]*receipt{}
	}
	if previous := d.byKey[e.IdempotencyKey]; previous != nil {
		// A key may be reused once the old receipt has expired or been evicted.
		// Recovery scans partitions, so use timestamp order instead of scan order.
		if previous.timestamp.After(e.Timestamp) || previous.timestamp.Equal(e.Timestamp) && previous.id >= e.ID {
			return nil
		}
		d.remove(previous)
	}
	r := &receipt{key: e.IdempotencyKey, hash: hash, partition: e.Partition, offset: e.Offset, timestamp: e.Timestamp, id: e.ID}
	d.byKey[r.key] = r
	heap.Push(&d.oldest, r)
	if len(d.oldest) > IdempotencyCapacity {
		d.remove(d.oldest[0])
	}
	return nil
}
