// Package groups coordinates fenced partition leases with durable next-offset commits.
package groups

import (
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/The-Null-Catchers/EventCore/internal/storage"
)

var ErrFenced = errors.New("consumer generation or delivery lease is stale")

type Store interface {
	Load() ([]State, error)
	Save(State) error
}
type State struct {
	Workspace string        `json:"workspace"`
	Topic     string        `json:"topic"`
	Name      string        `json:"name"`
	Epoch     uint64        `json:"epoch"`
	Offsets   map[int]int64 `json:"offsets"`
}
type Member struct {
	ID         string    `json:"id"`
	Expires    time.Time `json:"expires_at"`
	Partitions []int     `json:"partitions"`
}
type Snapshot struct {
	State
	Members []Member      `json:"members"`
	Lag     map[int]int64 `json:"lag"`
}
type Delivery struct {
	Partition int             `json:"partition"`
	Token     string          `json:"token"`
	Epoch     uint64          `json:"epoch"`
	Events    []storage.Event `json:"events"`
}
type lease struct {
	token, member string
	next          int64
	expires       time.Time
}
type group struct {
	state   State
	members map[string]time.Time
	pending map[int]lease
}
type Coordinator struct {
	mu              sync.Mutex
	broker          *storage.Broker
	store           Store
	groups          map[string]*group
	ttl, visibility time.Duration
	now             func() time.Time
}

func groupKey(w, t, g string) string { return w + "/" + t + "/" + g }
func clone(s State) State {
	c := s
	c.Offsets = map[int]int64{}
	for p, o := range s.Offsets {
		c.Offsets[p] = o
	}
	return c
}
func New(b *storage.Broker, s Store, ttl, visibility time.Duration) (*Coordinator, error) {
	c := &Coordinator{broker: b, store: s, groups: map[string]*group{}, ttl: ttl, visibility: visibility, now: time.Now}
	states, err := s.Load()
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		state.Epoch++
		if err = s.Save(state); err != nil {
			return nil, err
		}
		c.groups[groupKey(state.Workspace, state.Topic, state.Name)] = &group{state: state, members: map[string]time.Time{}, pending: map[int]lease{}}
	}
	return c, nil
}
func (c *Coordinator) Create(w, t, n, start string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !storage.ValidName(n) {
		return errors.New("invalid group name")
	}
	k := groupKey(w, t, n)
	if _, ok := c.groups[k]; ok {
		return errors.New("group exists")
	}
	cfg, err := c.broker.Config(w, t)
	if err != nil {
		return err
	}
	if start != "earliest" && start != "latest" {
		return errors.New("start must be earliest or latest")
	}
	s := State{Workspace: w, Topic: t, Name: n, Epoch: 1, Offsets: map[int]int64{}}
	for p := 0; p < cfg.Partitions; p++ {
		bounds, err := c.broker.Bounds(w, t, p)
		if err != nil {
			return err
		}
		s.Offsets[p] = bounds.Oldest
		if start == "latest" {
			s.Offsets[p] = bounds.Next
		}
	}
	if err = c.store.Save(s); err != nil {
		return err
	}
	c.groups[k] = &group{state: s, members: map[string]time.Time{}, pending: map[int]lease{}}
	return nil
}
func (c *Coordinator) get(w, t, n string) (*group, error) {
	g, ok := c.groups[groupKey(w, t, n)]
	if !ok {
		return nil, errors.New("group not found")
	}
	return g, nil
}
func (c *Coordinator) bump(g *group) error {
	s := clone(g.state)
	s.Epoch++
	if err := c.store.Save(s); err != nil {
		return err
	}
	g.state = s
	g.pending = map[int]lease{}
	return nil
}
func (c *Coordinator) expire(g *group) error {
	expired := []string{}
	for id, until := range g.members {
		if !until.After(c.now()) {
			expired = append(expired, id)
		}
	}
	if len(expired) == 0 {
		return nil
	}
	if err := c.bump(g); err != nil {
		return err
	}
	for _, id := range expired {
		delete(g.members, id)
	}
	return nil
}
func owners(g *group) map[int]string {
	ids := []string{}
	for id := range g.members {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := map[int]string{}
	if len(ids) > 0 {
		for p := range g.state.Offsets {
			out[p] = ids[p%len(ids)]
		}
	}
	return out
}
func (c *Coordinator) snapshot(g *group) (Snapshot, error) {
	s := Snapshot{State: clone(g.state), Members: []Member{}, Lag: map[int]int64{}}
	owned := owners(g)
	for id, until := range g.members {
		m := Member{ID: id, Expires: until, Partitions: []int{}}
		for p, owner := range owned {
			if owner == id {
				m.Partitions = append(m.Partitions, p)
			}
		}
		sort.Ints(m.Partitions)
		s.Members = append(s.Members, m)
	}
	sort.Slice(s.Members, func(i, j int) bool { return s.Members[i].ID < s.Members[j].ID })
	for p, o := range g.state.Offsets {
		bounds, err := c.broker.Bounds(g.state.Workspace, g.state.Topic, p)
		if err != nil {
			return s, err
		}
		s.Lag[p] = bounds.Next - o
	}
	return s, nil
}
func (c *Coordinator) Join(w, t, n, id string) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !storage.ValidName(id) {
		return Snapshot{}, errors.New("invalid member ID")
	}
	g, err := c.get(w, t, n)
	if err != nil {
		return Snapshot{}, err
	}
	if err = c.expire(g); err != nil {
		return Snapshot{}, err
	}
	if _, exists := g.members[id]; !exists {
		if len(g.members) >= 256 {
			return Snapshot{}, errors.New("member limit")
		}
		if err = c.bump(g); err != nil {
			return Snapshot{}, err
		}
	}
	g.members[id] = c.now().Add(c.ttl)
	return c.snapshot(g)
}
func (c *Coordinator) Leave(w, t, n, id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.get(w, t, n)
	if err != nil {
		return err
	}
	if _, ok := g.members[id]; !ok {
		return nil
	}
	if err = c.bump(g); err != nil {
		return err
	}
	delete(g.members, id)
	return nil
}
func (c *Coordinator) Inspect(w, t, n string) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.get(w, t, n)
	if err != nil {
		return Snapshot{}, err
	}
	if err = c.expire(g); err != nil {
		return Snapshot{}, err
	}
	return c.snapshot(g)
}
func (c *Coordinator) Pull(w, t, n, id string, epoch uint64, limit int) ([]Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.get(w, t, n)
	if err != nil {
		return nil, err
	}
	if err = c.expire(g); err != nil {
		return nil, err
	}
	if epoch != g.state.Epoch || !g.members[id].After(c.now()) {
		return nil, ErrFenced
	}
	g.members[id] = c.now().Add(c.ttl)
	out := []Delivery{}
	owned := owners(g)
	for p := 0; p < len(g.state.Offsets); p++ {
		if owned[p] != id {
			continue
		}
		if pending, ok := g.pending[p]; ok && pending.expires.After(c.now()) {
			continue
		}
		events, err := c.broker.Read(w, t, p, g.state.Offsets[p], limit)
		if err != nil {
			return nil, fmt.Errorf("partition %d: %w", p, err)
		}
		if len(events) == 0 {
			continue
		}
		token := storage.ID()
		g.pending[p] = lease{token: token, member: id, next: events[len(events)-1].Offset + 1, expires: c.now().Add(c.visibility)}
		out = append(out, Delivery{Partition: p, Token: token, Epoch: epoch, Events: events})
		if len(out) >= 8 {
			break
		}
	}
	return out, nil
}
func (c *Coordinator) Ack(w, t, n, id string, epoch uint64, p int, token string, nack bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.get(w, t, n)
	if err != nil {
		return err
	}
	if err = c.expire(g); err != nil {
		return err
	}
	l, ok := g.pending[p]
	if !ok || epoch != g.state.Epoch || l.member != id || l.token != token || !l.expires.After(c.now()) || owners(g)[p] != id {
		return ErrFenced
	}
	if !nack {
		s := clone(g.state)
		s.Offsets[p] = l.next
		if err = c.store.Save(s); err != nil {
			return err
		}
		g.state = s
	}
	delete(g.pending, p)
	return nil
}

// Reset rejects live members, so administrative replay cannot race active processing.
func (c *Coordinator) Reset(w, t, n string, p int, offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	g, err := c.get(w, t, n)
	if err != nil {
		return err
	}
	if err = c.expire(g); err != nil {
		return err
	}
	if len(g.members) > 0 {
		return errors.New("stop all group members before reset")
	}
	if _, ok := g.state.Offsets[p]; !ok {
		return errors.New("invalid partition")
	}
	bounds, err := c.broker.Bounds(w, t, p)
	if err != nil {
		return err
	}
	if offset < bounds.Oldest || offset > bounds.Next {
		return storage.ErrRange
	}
	s := clone(g.state)
	s.Epoch++
	s.Offsets[p] = offset
	if err = c.store.Save(s); err != nil {
		return err
	}
	g.state = s
	g.pending = map[int]lease{}
	return nil
}
