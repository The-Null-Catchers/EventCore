package groups

import (
	"errors"
	"sort"
)

type PartitionMetrics struct {
	Partition                                   int
	Committed, Latest, Oldest, Lag, Unavailable int64
	InFlight                                    bool
}
type Metrics struct {
	Topic, Group string
	Members      int
	Consumed     uint64
	Partitions   []PartitionMetrics
}

// Metrics excludes expired memberships and leases without changing generations
// or writing metadata as a side effect of an observability scrape.
func (c *Coordinator) Metrics(w string) ([]Metrics, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := []Metrics{}
	now := c.now()
	for _, g := range c.groups {
		if g.state.Workspace != w {
			continue
		}
		m := Metrics{Topic: g.state.Topic, Group: g.state.Name, Consumed: g.consumed, Partitions: []PartitionMetrics{}}
		for _, until := range g.members {
			if until.After(now) {
				m.Members++
			}
		}
		for p, offset := range g.state.Offsets {
			bounds, err := c.broker.Bounds(w, g.state.Topic, p)
			if err != nil {
				return nil, err
			}
			if offset < 0 || offset > bounds.Next {
				return nil, errors.New("group committed offset is outside log high watermark")
			}
			v := PartitionMetrics{Partition: p, Committed: offset, Latest: bounds.Next, Oldest: bounds.Oldest, Lag: bounds.Next - offset}
			if offset < bounds.Oldest {
				v.Unavailable = bounds.Oldest - offset
			}
			if lease, ok := g.pending[p]; ok && lease.expires.After(now) && g.members[lease.member].After(now) {
				v.InFlight = true
			}
			m.Partitions = append(m.Partitions, v)
		}
		sort.Slice(m.Partitions, func(i, j int) bool { return m.Partitions[i].Partition < m.Partitions[j].Partition })
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic == out[j].Topic {
			return out[i].Group < out[j].Group
		}
		return out[i].Topic < out[j].Topic
	})
	return out, nil
}
