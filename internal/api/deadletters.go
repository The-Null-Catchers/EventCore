package api

import (
	"errors"
	"fmt"
	"github.com/The-Null-Catchers/EventCore/internal/deadletters"
	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"net/http"
	"strings"
)

func (s *Server) deadLetters(w http.ResponseWriter, r *http.Request, p metadata.Principal, topic string, parts []string) {
	if s.DeadLetters == nil {
		fail(w, 503, errors.New("DLQ resolution service unavailable"))
		return
	}
	if !strings.HasSuffix(topic, ".DLQ") {
		fail(w, 400, deadletters.ErrInvalid)
		return
	}
	if len(parts) == 4 && r.Method == "GET" {
		q, err := scanQuery(r)
		if err != nil {
			fail(w, 400, err)
			return
		}
		if q.Limit > 100 {
			fail(w, 400, errors.New("maximum 100 scanned records"))
			return
		}
		page, err := s.Broker.Scan(p.Workspace, topic, q)
		if err != nil {
			fail(w, replayStatus(err), err)
			return
		}
		type entry struct {
			Event    storage.Event         `json:"event"`
			Decision *deadletters.Decision `json:"decision,omitempty"`
			Error    string                `json:"error,omitempty"`
		}
		entries := []entry{}
		for _, e := range page.Events {
			item := entry{Event: e}
			if _, err := deadletters.Parse(topic, e); err != nil {
				item.Error = err.Error()
			}
			d, ok, err := s.DeadLetters.Store.DLQDecision(r.Context(), p.Workspace, topic, e.Partition, e.Offset)
			if err != nil {
				s.internal(w, err)
				return
			}
			if ok {
				d.Candidate = nil
				item.Decision = &d
			}
			entries = append(entries, item)
		}
		reply(w, 200, map[string]any{"entries": entries, "next_offset": page.NextOffset, "end_offset": page.EndOffset, "scanned": page.Scanned, "done": page.Done})
		return
	}
	if len(parts) != 5 || r.Method != "POST" || (parts[4] != "retry" && parts[4] != "discard") {
		fail(w, 404, errors.New("route not found"))
		return
	}
	if parts[4] == "retry" && !Allowed(p, strings.TrimSuffix(topic, ".DLQ"), "produce") {
		fail(w, 403, errors.New("original topic produce scope required"))
		return
	}
	var req struct {
		Partition *int   `json:"partition"`
		Offset    *int64 `json:"offset"`
		Confirm   bool   `json:"confirm"`
	}
	if err := body(w, r, &req); err != nil {
		fail(w, 400, err)
		return
	}
	if !req.Confirm || req.Partition == nil || req.Offset == nil {
		fail(w, 400, errors.New("partition, offset and confirm=true required"))
		return
	}
	if !s.audit(w, r, p, "dlq."+parts[4], fmt.Sprintf("%s/%d/%d", topic, *req.Partition, *req.Offset)) {
		return
	}
	d, err := s.DeadLetters.Resolve(r.Context(), p.Workspace, topic, p.ID, *req.Partition, *req.Offset, parts[4])
	if err != nil {
		status := replayStatus(err)
		if errors.Is(err, deadletters.ErrConflict) {
			status = 409
		}
		if errors.Is(err, deadletters.ErrStore) {
			status = 503
		}
		fail(w, status, err)
		return
	}
	d.Candidate = nil
	reply(w, 200, d)
}
