package metadata

import (
	"context"
	"database/sql"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
)

func (d *DB) WebhookMetrics(ctx context.Context, w string) (webhooks.Metrics, error) {
	var m webhooks.Metrics
	err := d.SQL.QueryRowContext(ctx, `SELECT delivered,failed,unknown,dead_letters FROM webhook_metrics WHERE workspace_id=$1`, w).Scan(&m.Delivered, &m.Failed, &m.Unknown, &m.DeadLetters)
	if errors.Is(err, sql.ErrNoRows) {
		return m, nil
	}
	return m, err
}
