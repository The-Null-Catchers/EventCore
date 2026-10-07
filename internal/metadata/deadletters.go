package metadata

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/deadletters"
)

func (d *DB) DLQDecision(ctx context.Context, w, topic string, partition int, offset int64) (deadletters.Decision, bool, error) {
	var raw []byte
	var decision deadletters.Decision
	err := d.SQL.QueryRowContext(ctx, `SELECT decision FROM dlq_resolutions WHERE workspace_id=$1 AND topic=$2 AND partition=$3 AND offset_id=$4`, w, topic, partition, offset).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return decision, false, nil
	}
	if err != nil {
		return decision, false, err
	}
	err = json.Unmarshal(raw, &decision)
	return decision, true, err
}
func (d *DB) SaveDLQDecision(ctx context.Context, v deadletters.Decision) error {
	raw, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = d.SQL.ExecContext(ctx, `INSERT INTO dlq_resolutions(workspace_id,topic,partition,offset_id,decision) VALUES($1,$2,$3,$4,$5) ON CONFLICT(workspace_id,topic,partition,offset_id) DO UPDATE SET decision=EXCLUDED.decision`, v.Workspace, v.Topic, v.Partition, v.Offset, raw)
	return err
}
func (d *DB) NextPendingDLQ(ctx context.Context) (deadletters.Decision, bool, error) {
	var raw []byte
	var v deadletters.Decision
	err := d.SQL.QueryRowContext(ctx, `SELECT decision FROM dlq_resolutions WHERE decision->>'status'='pending' ORDER BY workspace_id,topic,partition,offset_id LIMIT 1`).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	err = json.Unmarshal(raw, &v)
	return v, true, err
}
