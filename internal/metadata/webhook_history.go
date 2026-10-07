package metadata

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
)

func (d *DB) DeliveryHistory(ctx context.Context, w, id string, after int64, limit int) (webhooks.HistoryPage, error) {
	page := webhooks.HistoryPage{Entries: []webhooks.HistoryEntry{}, NextCursor: after}
	if after < 0 || limit < 1 || limit > 100 {
		return page, errors.New("invalid history cursor or limit")
	}
	rows, err := d.SQL.QueryContext(ctx, `SELECT a.id,a.event_id,a.attempt,a.recorded_at FROM webhook_delivery_history a JOIN webhooks h ON h.id=a.subscription_id WHERE h.workspace_id=$1 AND h.id=$2 AND a.id>$3 ORDER BY a.id LIMIT $4`, w, id, after, limit+1)
	if err != nil {
		return page, err
	}
	defer rows.Close()
	for rows.Next() {
		var e webhooks.HistoryEntry
		var raw []byte
		if err = rows.Scan(&e.Cursor, &e.EventID, &raw, &e.RecordedAt); err != nil {
			return page, err
		}
		if len(page.Entries) == limit {
			page.HasMore = true
			break
		}
		if err = json.Unmarshal(raw, &e.Attempt); err != nil {
			return page, err
		}
		page.Entries = append(page.Entries, e)
		page.NextCursor = e.Cursor
	}
	return page, rows.Err()
}
