package storage

import (
	"sort"
	"strings"
	"sync"
	"time"
)

var publishBuckets = [8]float64{0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 1, 10}

type PublishMetrics struct {
	Events, Bytes, DeadLetters uint64
	Seconds                    float64
	Buckets                    [8]uint64
}
type publishStats struct {
	mu    sync.Mutex
	value PublishMetrics
}

func (s *publishStats) observe(elapsed time.Duration, bytes int, deadLetter bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.value.Events++
	s.value.Bytes += uint64(bytes)
	if deadLetter {
		s.value.DeadLetters++
	}
	seconds := elapsed.Seconds()
	s.value.Seconds += seconds
	for i, b := range publishBuckets {
		if seconds <= b {
			s.value.Buckets[i]++
		}
	}
}

type TopicMetrics struct {
	Topic      string
	Publish    PublishMetrics
	Partitions []Bounds
}

// Metrics reads metadata only. It does not scan retained payloads or fabricate
// historical process counters from offsets after restart.
func (b *Broker) Metrics(w string) ([]TopicMetrics, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return nil, ErrClosed
	}
	out := []TopicMetrics{}
	for _, t := range b.topics {
		if t.config.Workspace != w {
			continue
		}
		m := TopicMetrics{Topic: t.config.Name, Partitions: []Bounds{}}
		t.stats.mu.Lock()
		m.Publish = t.stats.value
		t.stats.mu.Unlock()
		for _, p := range t.parts {
			p.mu.Lock()
			if p.poisoned != nil {
				p.mu.Unlock()
				return nil, ErrUnavailable
			}
			bounds := Bounds{Oldest: p.segments[0].base, Next: p.next, Events: p.next - p.segments[0].base, Segments: len(p.segments)}
			for _, s := range p.segments {
				bounds.Bytes += s.size
			}
			p.mu.Unlock()
			m.Partitions = append(m.Partitions, bounds)
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Topic < out[j].Topic })
	return out, nil
}
func isDeadLetter(topic string, in Input) bool {
	return strings.HasSuffix(topic, ".DLQ") && in.Type == "eventcore.delivery.failed"
}

func PublishLatencyBuckets() [8]float64 { return publishBuckets }
