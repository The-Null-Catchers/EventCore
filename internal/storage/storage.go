// Package storage implements fsync-before-ack segmented, checksummed event logs.
package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const maxRecord = 2 << 20

var nameRE = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$`)
var ErrRange = errors.New("offset outside retained range")
var ErrClosed = errors.New("broker closed")
var ErrUnavailable = errors.New("broker storage unavailable")

type Input struct {
	IdempotencyKey string            `json:"idempotency_key,omitempty"`
	Type           string            `json:"type"`
	Key            string            `json:"key,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Data           json.RawMessage   `json:"data"`
}
type Event struct {
	Deduplicated bool      `json:"deduplicated,omitempty"`
	ID           string    `json:"id"`
	Topic        string    `json:"topic"`
	Partition    int       `json:"partition"`
	Offset       int64     `json:"offset"`
	Timestamp    time.Time `json:"timestamp"`
	Input
}
type Topic struct {
	Schema           json.RawMessage `json:"schema,omitempty"`
	Workspace        string          `json:"workspace"`
	Name             string          `json:"name"`
	Description      string          `json:"description,omitempty"`
	Partitions       int             `json:"partitions"`
	MaxEventBytes    int             `json:"max_event_bytes"`
	RetentionBytes   int64           `json:"retention_bytes"`
	RetentionSeconds int64           `json:"retention_seconds"`
	CreatedAt        time.Time       `json:"created_at"`
}
type Bounds struct {
	Oldest   int64 `json:"oldest_offset"`
	Next     int64 `json:"next_offset"`
	Events   int64 `json:"events"`
	Bytes    int64 `json:"bytes"`
	Segments int   `json:"segments"`
}
type segment struct {
	base, next, size int64
	last             time.Time
	path             string
}
type partition struct {
	mu       sync.Mutex
	segments []segment
	file     *os.File
	next     int64
	poisoned error
	changed  chan struct{}
	prepared map[string]int64
}
type topicState struct {
	dedupMu sync.Mutex
	dedup   dedupIndex
	schema  *jsonschema.Schema
	config  Topic
	parts   []*partition
	rr      atomic.Uint64
}
type Broker struct {
	MinFreeBytes int64
	mu           sync.RWMutex
	root         string
	segmentBytes int64
	topics       map[string]*topicState
	lock         *os.File
	closed       bool
}

func ValidName(s string) bool { return nameRE.MatchString(s) && s != "." && s != ".." }
func key(w, n string) string  { return w + "/" + n }
func ID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// AtomicJSON makes metadata replacement durable before returning success.
func AtomicJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".pending-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err == nil {
		_, err = f.Write(b)
	}
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	return syncDir(filepath.Dir(path))
}
func Open(root string, segmentBytes int64) (*Broker, error) {
	if segmentBytes < 1024 {
		return nil, errors.New("segment size must be >= 1024")
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(root, "broker.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("data directory already locked: %w", err)
	}
	b := &Broker{MinFreeBytes: 64 << 20, root: root, segmentBytes: segmentBytes, topics: map[string]*topicState{}, lock: lock}
	ok := false
	defer func() {
		if !ok {
			b.Close()
		}
	}()
	paths, err := filepath.Glob(filepath.Join(root, "topics", "*", "*", "topic.json"))
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		raw, e := os.ReadFile(path)
		if e != nil {
			return nil, e
		}
		var c Topic
		if e = json.Unmarshal(raw, &c); e != nil {
			return nil, e
		}
		if !ValidName(c.Workspace) || !ValidName(c.Name) || c.Partitions < 1 || c.Partitions > 256 || c.MaxEventBytes < 1 || c.MaxEventBytes > maxRecord-4096 || path != filepath.Join(root, "topics", c.Workspace, c.Name, "topic.json") {
			return nil, fmt.Errorf("invalid topic manifest %s", path)
		}
		t, e := b.openTopic(c)
		if e != nil {
			return nil, e
		}
		b.topics[key(c.Workspace, c.Name)] = t
	}
	ok = true
	return b, nil
}
func (b *Broker) openTopic(c Topic) (*topicState, error) {
	t := &topicState{config: c}
	if len(c.Schema) > 0 {
		if len(c.Schema) > 64<<10 {
			return nil, errors.New("schema maximum 64 KiB")
		}
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(c.Schema))
		if err != nil {
			return nil, err
		}
		compiler := jsonschema.NewCompiler()
		compiler.DefaultDraft(jsonschema.Draft2020)
		compiler.UseLoader(denyLoader{})
		if err = compiler.AddResource("https://eventcore.invalid/schema", doc); err != nil {
			return nil, err
		}
		t.schema, err = compiler.Compile("https://eventcore.invalid/schema")
		if err != nil {
			return nil, err
		}
	}

	for i := 0; i < c.Partitions; i++ {
		p, err := openPartition(filepath.Join(b.root, "topics", c.Workspace, c.Name, strconv.Itoa(i)), func(e Event) error {
			if e.Topic != c.Name || e.Partition != i {
				return errors.New("record topic/partition mismatch")
			}
			return t.dedup.remember(e, time.Now())
		})
		if err != nil {
			for _, old := range t.parts {
				old.file.Close()
			}
			return nil, fmt.Errorf("%s partition %d: %w", c.Name, i, err)
		}
		t.parts = append(t.parts, p)
	}
	return t, nil
}
func (b *Broker) Create(c Topic) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ErrClosed
	}
	if !ValidName(c.Workspace) || !ValidName(c.Name) || c.Partitions < 1 || c.Partitions > 256 {
		return errors.New("invalid workspace, topic name or partition count")
	}
	if _, ok := b.topics[key(c.Workspace, c.Name)]; ok {
		return errors.New("topic exists")
	}
	if c.MaxEventBytes == 0 {
		c.MaxEventBytes = 1 << 20
	}
	if c.MaxEventBytes < 1 || c.MaxEventBytes > maxRecord-4096 || c.RetentionBytes < 0 || c.RetentionSeconds < 0 {
		return errors.New("invalid topic limits")
	}
	c.CreatedAt = time.Now().UTC()
	dir := filepath.Join(b.root, "topics", c.Workspace, c.Name)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	t, err := b.openTopic(c)
	if err != nil {
		return err
	}
	if err = AtomicJSON(filepath.Join(dir, "topic.json"), c); err != nil {
		for _, p := range t.parts {
			p.file.Close()
		}
		return err
	}
	if err = syncDir(filepath.Dir(dir)); err != nil {
		return err
	}
	if err = syncDir(filepath.Join(b.root, "topics")); err != nil {
		return err
	}
	if err = syncDir(b.root); err != nil {
		return err
	}
	b.topics[key(c.Workspace, c.Name)] = t
	return nil
}
func (b *Broker) Topics(workspace string) []Topic {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := []Topic{}
	for _, t := range b.topics {
		if t.config.Workspace == workspace {
			out = append(out, t.config)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
func (b *Broker) topic(w, n string) (*topicState, error) {
	if b.closed {
		return nil, ErrClosed
	}
	t, ok := b.topics[key(w, n)]
	if !ok {
		return nil, errors.New("topic not found")
	}
	return t, nil
}
func (b *Broker) Config(w, n string) (Topic, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, err := b.topic(w, n)
	if err != nil {
		return Topic{}, err
	}
	return t.config, nil
}
func PartitionFor(key string, n int) int {
	h := fnv.New64a()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(n))
}
func (b *Broker) Publish(w, n string, in Input) (Event, error) {
	return b.publish(w, n, in, nil, nil)
}

// PublishPrepared reserves a concrete receipt in durable metadata before append.
// The callback runs under the partition lock and must not re-enter this broker.
func (b *Broker) PublishPrepared(w, n string, in Input, prepare func(Event) error) (Event, error) {
	if prepare == nil || in.IdempotencyKey != "" {
		return Event{}, errors.New("prepare callback required; producer idempotency key must be empty")
	}
	return b.publish(w, n, in, prepare, nil)
}

// RestorePrepared verifies the reserved offset before finishing an interrupted
// append. It never appends again if the exact receipt is already present.
func (b *Broker) RestorePrepared(w string, e Event) (Event, error) {
	if !ValidName(e.ID) || e.Timestamp.IsZero() || e.IdempotencyKey != "" {
		return Event{}, errors.New("invalid prepared receipt")
	}
	e.Deduplicated = false
	return b.publish(w, e.Topic, e.Input, nil, &e)
}
func (b *Broker) publish(w, n string, in Input, prepare func(Event) error, expected *Event) (Event, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, err := b.topic(w, n)
	if err != nil {
		return Event{}, err
	}
	if in.Type == "" || len(in.Type) > 256 || !json.Valid(in.Data) || len(in.Key) > 4096 || len(in.Headers) > 64 || len(in.IdempotencyKey) > 256 {
		return Event{}, errors.New("invalid event envelope")
	}
	if t.schema != nil {
		doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(in.Data))
		if err != nil {
			return Event{}, err
		}
		if err = t.schema.Validate(doc); err != nil {
			return Event{}, fmt.Errorf("schema validation: %w", err)
		}
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return Event{}, err
	}
	if len(raw) > t.config.MaxEventBytes {
		return Event{}, errors.New("event exceeds topic maximum")
	}
	if in.IdempotencyKey != "" {
		// Serializes keyed requests across partitions: one key is scoped to the
		// workspace/topic, including unkeyed round-robin publications.
		t.dedupMu.Lock()
		defer t.dedupMu.Unlock()
		t.dedup.prune(time.Now())
		if previous := t.dedup.byKey[in.IdempotencyKey]; previous != nil {
			p := t.parts[previous.partition]
			p.mu.Lock()
			if p.poisoned != nil {
				p.mu.Unlock()
				return Event{}, ErrUnavailable
			}
			if previous.offset < p.segments[0].base {
				t.dedup.remove(previous)
				p.mu.Unlock()
			} else {
				hash, err := fingerprint(in)
				if err != nil {
					p.mu.Unlock()
					return Event{}, err
				}
				if hash != previous.hash {
					p.mu.Unlock()
					return Event{}, ErrIdempotencyConflict
				}
				events, err := p.read(previous.offset, 1)
				if err != nil || len(events) != 1 || events[0].ID != previous.id {
					p.poisoned = errors.New("idempotency receipt unavailable")
					p.mu.Unlock()
					return Event{}, fmt.Errorf("%w: idempotency receipt unavailable", ErrUnavailable)
				}
				p.mu.Unlock()
				events[0].Deduplicated = true
				return events[0], nil
			}
		}
	}
	idx := 0
	if expected != nil {
		idx = expected.Partition
		if idx < 0 || idx >= len(t.parts) {
			return Event{}, errors.New("invalid prepared partition")
		}
	} else if in.Key != "" {
		idx = PartitionFor(in.Key, len(t.parts))
	} else {
		idx = int((t.rr.Add(1) - 1) % uint64(len(t.parts)))
	}
	p := t.parts[idx]
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.poisoned != nil {
		return Event{}, fmt.Errorf("%w: partition requires recovery", ErrUnavailable)
	}
	if expected != nil {
		if expected.Offset < p.segments[0].base || expected.Offset > p.next {
			return Event{}, fmt.Errorf("%w: prepared offset outside retained log", ErrUnavailable)
		}
		if expected.Offset < p.next {
			events, err := p.read(expected.Offset, 1)
			if err != nil || len(events) != 1 || events[0].ID != expected.ID {
				p.poisoned = errors.New("prepared offset contains a different receipt")
				return Event{}, fmt.Errorf("%w: %v", ErrUnavailable, p.poisoned)
			}
			actualHash, actualErr := fingerprint(events[0].Input)
			expectedHash, expectedErr := fingerprint(expected.Input)
			if actualErr != nil || expectedErr != nil || actualHash != expectedHash || events[0].Topic != expected.Topic || !events[0].Timestamp.Equal(expected.Timestamp) {
				p.poisoned = errors.New("prepared receipt content mismatch")
				return Event{}, fmt.Errorf("%w: %v", ErrUnavailable, p.poisoned)
			}
			if p.prepared == nil {
				p.prepared = map[string]int64{}
			}
			p.prepared[expected.ID] = expected.Offset
			events[0].Deduplicated = true
			return events[0], nil
		}
	}
	usage, err := b.disk()
	if err != nil {
		return Event{}, fmt.Errorf("%w: capacity check failed", ErrUnavailable)
	}
	if usage.AvailableBytes < b.MinFreeBytes || usage.AvailableBytes-b.MinFreeBytes < maxRecord {
		return Event{}, fmt.Errorf("%w: minimum free disk threshold reached", ErrUnavailable)
	}
	e := Event{ID: ID(), Topic: n, Partition: idx, Offset: p.next, Timestamp: time.Now().UTC(), Input: in}
	if expected != nil {
		e = *expected
	}
	payload, err := json.Marshal(e)
	if err != nil {
		return Event{}, err
	}
	if len(payload) > maxRecord {
		return Event{}, errors.New("encoded record too large")
	}
	if prepare != nil {
		if err = prepare(e); err != nil {
			// A failed metadata write may nevertheless have committed remotely.
			// Fence the reservation until startup recovery resolves that ambiguity.
			p.poisoned = err
			return Event{}, fmt.Errorf("%w: prepared metadata write failed; restart required", ErrUnavailable)
		}
	}
	if prepare != nil || expected != nil {
		if p.prepared == nil {
			p.prepared = map[string]int64{}
		}
		p.prepared[e.ID] = e.Offset
	}
	s := &p.segments[len(p.segments)-1]
	if s.size > 0 && s.size+int64(len(payload)+8) > b.segmentBytes {
		if err = p.rotate(); err != nil {
			p.poisoned = err
			return Event{}, fmt.Errorf("%w: segment rotation failed", ErrUnavailable)
		}
		s = &p.segments[len(p.segments)-1]
	}
	header := make([]byte, 8)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	record := append(header, payload...)
	written, err := p.file.Write(record)
	if err == nil && written != len(record) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = p.file.Sync()
	}
	if err != nil {
		p.poisoned = err
		return Event{}, fmt.Errorf("%w: append/fsync failed; partition fenced until recovery", ErrUnavailable)
	}
	if err = t.dedupRemember(e); err != nil {
		p.poisoned = err
		return Event{}, ErrUnavailable
	}
	p.next++
	s.next = p.next
	s.size += int64(len(record))
	s.last = e.Timestamp
	close(p.changed)
	p.changed = make(chan struct{})
	return e, nil
}
func (t *topicState) dedupRemember(e Event) error {
	if e.IdempotencyKey == "" {
		return nil
	}
	return t.dedup.remember(e, time.Now())
}
func openPartition(dir string, recoverEvent func(Event) error) (*partition, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.log"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	p := &partition{changed: make(chan struct{})}
	if len(paths) == 0 {
		p.segments = []segment{{path: filepath.Join(dir, fmt.Sprintf("%020d.log", 0))}}
		if err = p.openActive(); err != nil {
			return nil, err
		}
		return p, nil
	}
	for i, path := range paths {
		base, err := strconv.ParseInt(strings.TrimSuffix(filepath.Base(path), ".log"), 10, 64)
		if err != nil || base < 0 {
			return nil, errors.New("invalid segment filename")
		}
		if i > 0 && base != p.next {
			return nil, errors.New("noncontiguous segment bases")
		}
		s := segment{base: base, next: base, path: path}
		f, err := os.OpenFile(path, os.O_RDWR, 0600)
		if err != nil {
			return nil, err
		}
		position := int64(0)
		for {
			e, size, readErr := decode(f)
			if readErr == io.EOF {
				break
			}
			if readErr == io.ErrUnexpectedEOF && i == len(paths)-1 {
				if err = f.Truncate(position); err == nil {
					err = f.Sync()
				}
				if err != nil {
					f.Close()
					return nil, err
				}
				break
			}
			if readErr != nil {
				f.Close()
				return nil, fmt.Errorf("corrupt segment %s at byte %d: %w", path, position, readErr)
			}
			if e.Offset != s.next {
				f.Close()
				return nil, errors.New("noncontiguous offsets")
			}
			if err = recoverEvent(e); err != nil {
				f.Close()
				return nil, err
			}
			s.next++
			s.last = e.Timestamp
			position += size
		}
		f.Close()
		s.size = position
		p.next = s.next
		p.segments = append(p.segments, s)
	}
	if err = p.openActive(); err != nil {
		return nil, err
	}
	return p, nil
}
func decode(r io.Reader) (Event, int64, error) {
	var e Event
	var h [8]byte
	_, err := io.ReadFull(r, h[:])
	if err != nil {
		return e, 0, err
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > maxRecord {
		return e, 0, errors.New("invalid record length")
	}
	raw := make([]byte, n)
	if _, err = io.ReadFull(r, raw); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return e, 0, err
	}
	if crc32.ChecksumIEEE(raw) != binary.BigEndian.Uint32(h[4:]) {
		return e, 0, errors.New("checksum mismatch")
	}
	if err = json.Unmarshal(raw, &e); err != nil {
		return e, 0, err
	}
	return e, int64(n) + 8, nil
}
func (p *partition) openActive() error {
	s := &p.segments[len(p.segments)-1]
	f, err := os.OpenFile(s.path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	p.file = f
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	return syncDir(filepath.Dir(s.path))
}
func (p *partition) rotate() error {
	if err := p.file.Close(); err != nil {
		return err
	}
	p.segments = append(p.segments, segment{base: p.next, next: p.next, path: filepath.Join(filepath.Dir(p.segments[0].path), fmt.Sprintf("%020d.log", p.next))})
	return p.openActive()
}
func (b *Broker) Bounds(w, n string, idx int) (Bounds, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, err := b.topic(w, n)
	if err != nil {
		return Bounds{}, err
	}
	if idx < 0 || idx >= len(t.parts) {
		return Bounds{}, errors.New("invalid partition")
	}
	p := t.parts[idx]
	p.mu.Lock()
	defer p.mu.Unlock()
	out := Bounds{Oldest: p.segments[0].base, Next: p.next, Events: p.next - p.segments[0].base, Segments: len(p.segments)}
	for _, s := range p.segments {
		out.Bytes += s.size
	}
	return out, nil
}
func (b *Broker) Read(w, n string, idx int, offset int64, limit int) ([]Event, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, err := b.topic(w, n)
	if err != nil {
		return nil, err
	}
	if idx < 0 || idx >= len(t.parts) || limit < 1 || limit > 1000 {
		return nil, errors.New("invalid partition or limit")
	}
	p := t.parts[idx]
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.read(offset, limit)
}

// read requires the partition mutex; reads at most one bounded page from disk.
func (p *partition) read(offset int64, limit int) ([]Event, error) {
	if offset < p.segments[0].base || offset > p.next {
		return nil, ErrRange
	}
	out := []Event{}
	bytes := 0
	for _, s := range p.segments {
		if s.next <= offset {
			continue
		}
		f, err := os.Open(s.path)
		if err != nil {
			return nil, err
		}
		for {
			e, size, err := decode(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return nil, err
			}
			if e.Offset >= offset {
				out = append(out, e)
				bytes += int(size)
				if len(out) >= limit || bytes >= 4<<20 {
					f.Close()
					return out, nil
				}
			}
		}
		f.Close()
	}
	return out, nil
}

// Retain deletes only sealed segments. Limits are per partition; active segments are retained.
func (b *Broker) Retain(now time.Time) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrClosed
	}
	for _, t := range b.topics {
		for _, p := range t.parts {
			p.mu.Lock()
			var total int64
			for _, s := range p.segments {
				total += s.size
			}
			for len(p.segments) > 1 {
				s := p.segments[0]
				pinned := false
				for _, offset := range p.prepared {
					if offset < s.next {
						pinned = true
						break
					}
				}
				if pinned {
					break
				}
				expired := t.config.RetentionSeconds > 0 && now.Sub(s.last) >= time.Duration(t.config.RetentionSeconds)*time.Second
				oversize := t.config.RetentionBytes > 0 && total > t.config.RetentionBytes/int64(len(t.parts))
				if !expired && !oversize {
					break
				}
				if err := os.Remove(s.path); err != nil {
					p.mu.Unlock()
					return err
				}
				p.segments = p.segments[1:]
				total -= s.size
				if err := syncDir(filepath.Dir(s.path)); err != nil {
					p.mu.Unlock()
					return err
				}
			}
			p.mu.Unlock()
		}
	}
	return nil
}
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil
	}
	b.closed = true
	var errs []error
	for _, t := range b.topics {
		for _, p := range t.parts {
			close(p.changed)
			if p.file != nil {
				errs = append(errs, p.file.Close())
			}
		}
	}
	if b.lock != nil {
		errs = append(errs, syscall.Flock(int(b.lock.Fd()), syscall.LOCK_UN), b.lock.Close())
	}
	return errors.Join(errs...)
}

type denyLoader struct{}

func (denyLoader) Load(string) (any, error) {
	return nil, errors.New("external schema references are prohibited")
}

// Wait uses bounded notification channels; events remain on disk, never in fanout buffers.
func (b *Broker) Wait(ctx context.Context, w, n string, idx int, offset int64) error {
	b.mu.RLock()
	t, err := b.topic(w, n)
	if err != nil {
		b.mu.RUnlock()
		return err
	}
	if idx < 0 || idx >= len(t.parts) {
		b.mu.RUnlock()
		return errors.New("invalid partition")
	}
	p := t.parts[idx]
	p.mu.Lock()
	if offset < p.segments[0].base || offset > p.next {
		p.mu.Unlock()
		b.mu.RUnlock()
		return ErrRange
	}
	if p.next > offset {
		p.mu.Unlock()
		b.mu.RUnlock()
		return nil
	}
	changed := p.changed
	p.mu.Unlock()
	b.mu.RUnlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-changed:
		return nil
	}
}

type DiskUsage struct {
	TotalBytes     int64 `json:"total_bytes"`
	AvailableBytes int64 `json:"available_bytes"`
}

func (b *Broker) disk() (DiskUsage, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(b.root, &stat); err != nil {
		return DiskUsage{}, err
	}
	return DiskUsage{TotalBytes: int64(stat.Blocks) * stat.Bsize, AvailableBytes: int64(stat.Bavail) * stat.Bsize}, nil
}
func (b *Broker) Disk() (DiskUsage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return DiskUsage{}, ErrClosed
	}
	return b.disk()
}
func (b *Broker) Check() error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return ErrClosed
	}
	for _, t := range b.topics {
		for _, p := range t.parts {
			p.mu.Lock()
			err := p.poisoned
			p.mu.Unlock()
			if err != nil {
				return ErrUnavailable
			}
		}
	}
	usage, err := b.disk()
	if err != nil {
		return err
	}
	if usage.AvailableBytes < b.MinFreeBytes || usage.AvailableBytes-b.MinFreeBytes < maxRecord {
		return ErrUnavailable
	}
	return nil
}

// CompletePrepared releases the retention pin only after metadata completion.
func (b *Broker) CompletePrepared(w, n, id string, idx int) error {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, err := b.topic(w, n)
	if err != nil {
		return err
	}
	if idx < 0 || idx >= len(t.parts) {
		return errors.New("invalid partition")
	}
	p := t.parts[idx]
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.prepared, id)
	return nil
}
