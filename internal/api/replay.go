package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
)

func scanQuery(r *http.Request) (storage.Range, error) {
	var q storage.Range
	var err error
	values := r.URL.Query()
	if q.Partition, err = strconv.Atoi(values.Get("partition")); err != nil {
		return q, errors.New("partition required")
	}
	if q.Offset, err = strconv.ParseInt(values.Get("offset"), 10, 64); err != nil {
		return q, errors.New("offset required")
	}
	q.Limit = 100
	if v := values.Get("limit"); v != "" {
		if q.Limit, err = strconv.Atoi(v); err != nil {
			return q, err
		}
	}
	if v := values.Get("end_offset"); v != "" {
		end, e := strconv.ParseInt(v, 10, 64)
		if e != nil {
			return q, e
		}
		q.EndOffset = &end
	}
	for name, dest := range map[string]**time.Time{"from_time": &q.FromTime, "until_time": &q.UntilTime} {
		if v := values.Get(name); v != "" {
			parsed, e := time.Parse(time.RFC3339Nano, v)
			if e != nil {
				return q, errors.New(name + " must be RFC3339")
			}
			*dest = &parsed
		}
	}
	q.Type = values.Get("type")
	q.Key = values.Get("key")
	q.ID = values.Get("id")
	return q, nil
}
func replayStatus(err error) int {
	if errors.Is(err, storage.ErrRange) {
		return 416
	}
	if errors.Is(err, storage.ErrIdempotencyConflict) {
		return 409
	}
	if errors.Is(err, storage.ErrUnavailable) || errors.Is(err, storage.ErrClosed) {
		return 503
	}
	return 400
}
func (s *Server) export(w http.ResponseWriter, r *http.Request, p metadata.Principal, topic string) {
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
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", `attachment; filename="events.jsonl"`)
	w.Header().Set("X-EventCore-Next-Offset", strconv.FormatInt(page.NextOffset, 10))
	w.Header().Set("X-EventCore-End-Offset", strconv.FormatInt(page.EndOffset, 10))
	w.Header().Set("X-EventCore-Scanned", strconv.Itoa(page.Scanned))
	w.Header().Set("X-EventCore-Done", strconv.FormatBool(page.Done))
	encoder := json.NewEncoder(w)
	for _, event := range page.Events {
		if err = encoder.Encode(event); err != nil {
			return
		}
	}
}

type replayRequest struct {
	storage.Range
	Target   string `json:"target"`
	ReplayID string `json:"replay_id"`
	Confirm  bool   `json:"confirm"`
}
type replayReceipt struct {
	SourceOffset int64         `json:"source_offset"`
	Event        storage.Event `json:"event"`
}
type replayResult struct {
	Receipts   []replayReceipt `json:"receipts"`
	NextOffset int64           `json:"next_offset"`
	EndOffset  int64           `json:"end_offset"`
	Scanned    int             `json:"scanned"`
	Done       bool            `json:"done"`
	Error      string          `json:"error,omitempty"`
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request, p metadata.Principal, topic string) {
	var req replayRequest
	if err := body(w, r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	if !req.Confirm || !storage.ValidName(req.ReplayID) || !storage.ValidName(req.Target) || req.Target == topic || req.EndOffset == nil {
		fail(w, 400, errors.New("confirm=true, replay_id, a different target and end_offset required"))
		return
	}
	if !Allowed(p, req.Target, "produce") {
		fail(w, 403, errors.New("target produce scope required"))
		return
	}
	if req.Limit == 0 {
		req.Limit = 100
	}
	if req.Limit > 100 {
		fail(w, 400, errors.New("replay maximum 100 scanned events per request"))
		return
	}
	if _, err := s.Broker.Config(p.Workspace, req.Target); err != nil {
		fail(w, 404, err)
		return
	}
	page, err := s.Broker.Scan(p.Workspace, topic, req.Range)
	if err != nil {
		fail(w, replayStatus(err), err)
		return
	}
	if !s.audit(w, r, p, "events.replay", topic+"/"+req.ReplayID+"/"+req.Target) {
		return
	}
	result := replayResult{Receipts: []replayReceipt{}, NextOffset: req.Offset, EndOffset: page.EndOffset, Scanned: page.Scanned}
	for _, original := range page.Events {
		headers := map[string]string{}
		for k, v := range original.Headers {
			headers[k] = v
		}
		headers["eventcore.replay.id"] = req.ReplayID
		headers["eventcore.replay.source_id"] = original.ID
		headers["eventcore.replay.source_topic"] = topic
		headers["eventcore.replay.source_partition"] = strconv.Itoa(original.Partition)
		headers["eventcore.replay.source_offset"] = strconv.FormatInt(original.Offset, 10)
		// A request retry uses the same derived key; a distinct replay ID copies anew.
		identity, _ := json.Marshal([]any{req.ReplayID, topic, original.Partition, original.Offset, original.ID})
		hash := sha256.Sum256(identity)
		copied, e := s.Broker.Publish(p.Workspace, req.Target, storage.Input{Type: original.Type, Key: original.Key, Data: original.Data, Headers: headers, IdempotencyKey: "replay-" + hex.EncodeToString(hash[:])})
		if e != nil {
			result.NextOffset = original.Offset
			result.Error = e.Error()
			reply(w, replayStatus(e), result)
			return
		}
		result.Receipts = append(result.Receipts, replayReceipt{SourceOffset: original.Offset, Event: copied})
		result.NextOffset = original.Offset + 1
	}
	result.NextOffset = page.NextOffset
	result.Done = page.Done
	reply(w, 200, result)
}
