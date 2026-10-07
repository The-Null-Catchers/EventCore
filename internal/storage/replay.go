package storage

import (
	"errors"
	"fmt"
	"time"
)

// Range selects a bounded page, never an unbounded timestamp scan. EndOffset is
// exclusive; nil captures the partition's current high-water mark for this page.
type Range struct {
	Partition int        `json:"partition"`
	Offset    int64      `json:"offset"`
	EndOffset *int64     `json:"end_offset,omitempty"`
	Limit     int        `json:"limit"`
	FromTime  *time.Time `json:"from_time,omitempty"`
	UntilTime *time.Time `json:"until_time,omitempty"`
	Type      string     `json:"type,omitempty"`
	Key       string     `json:"key,omitempty"`
	ID        string     `json:"id,omitempty"`
}
type Page struct {
	Events     []Event `json:"events"`
	NextOffset int64   `json:"next_offset"`
	EndOffset  int64   `json:"end_offset"`
	Scanned    int     `json:"scanned"`
	Done       bool    `json:"done"`
}

func (b *Broker) Scan(w, n string, q Range) (Page, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, err := b.topic(w, n)
	if err != nil {
		return Page{}, err
	}
	if q.Partition < 0 || q.Partition >= len(t.parts) || q.Limit < 1 || q.Limit > 1000 {
		return Page{}, errors.New("invalid partition or scan limit")
	}
	if q.FromTime != nil && q.UntilTime != nil && !q.FromTime.Before(*q.UntilTime) {
		return Page{}, errors.New("from_time must precede until_time")
	}
	p := t.parts[q.Partition]
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poisoned != nil {
		return Page{}, ErrUnavailable
	}
	end := p.next
	if q.EndOffset != nil {
		end = *q.EndOffset
	}
	if q.Offset < p.segments[0].base || q.Offset > p.next || end < q.Offset || end > p.next {
		return Page{}, ErrRange
	}
	page := Page{Events: []Event{}, NextOffset: q.Offset, EndOffset: end, Done: q.Offset == end}
	if page.Done {
		return page, nil
	}
	events, err := p.read(q.Offset, q.Limit)
	if err != nil {
		p.poisoned = err
		return Page{}, fmt.Errorf("%w: scan failed", ErrUnavailable)
	}
	for _, e := range events {
		if e.Offset >= end {
			break
		}
		page.Scanned++
		page.NextOffset = e.Offset + 1
		if q.FromTime != nil && e.Timestamp.Before(*q.FromTime) || q.UntilTime != nil && !e.Timestamp.Before(*q.UntilTime) || q.Type != "" && q.Type != e.Type || q.Key != "" && q.Key != e.Key || q.ID != "" && q.ID != e.ID {
			continue
		}
		page.Events = append(page.Events, e)
	}
	page.Done = page.NextOffset == end
	return page, nil
}
