package metadata

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/crypto/bcrypt"
	"time"
)

type DB struct{ SQL *sql.DB }
type Principal struct {
	ID, Workspace, Role string
	Scopes              []string
	Cookie              bool
	CSRF                string
}

func Digest(s string) string { h := sha256.Sum256([]byte(s)); return hex.EncodeToString(h[:]) }
func Open(ctx context.Context, url string) (*DB, error) {
	db, err := sql.Open("pgx", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(4)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err = db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &DB{SQL: db}, nil
}

// Migrate serializes concurrent startup migrations using a transaction advisory lock.
func (d *DB) Migrate(ctx context.Context) error {
	tx, err := d.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(761324098)`); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS workspaces(id text PRIMARY KEY,created_at timestamptz NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS users(id text PRIMARY KEY,email text UNIQUE NOT NULL,password_hash text NOT NULL);
CREATE TABLE IF NOT EXISTS memberships(user_id text REFERENCES users(id),workspace_id text REFERENCES workspaces(id),role text NOT NULL CHECK(role IN ('owner','admin','developer','viewer')),PRIMARY KEY(user_id,workspace_id));
CREATE TABLE IF NOT EXISTS sessions(token_hash text PRIMARY KEY,user_id text REFERENCES users(id),workspace_id text REFERENCES workspaces(id),csrf text NOT NULL,expires_at timestamptz NOT NULL);
CREATE TABLE IF NOT EXISTS api_keys(id text PRIMARY KEY,workspace_id text REFERENCES workspaces(id),token_hash text UNIQUE NOT NULL,name text NOT NULL,scopes jsonb NOT NULL,expires_at timestamptz NOT NULL,revoked bool NOT NULL DEFAULT false,last_used_at timestamptz);
CREATE TABLE IF NOT EXISTS consumer_groups(workspace_id text REFERENCES workspaces(id),topic text NOT NULL,name text NOT NULL,state jsonb NOT NULL,PRIMARY KEY(workspace_id,topic,name));
CREATE TABLE IF NOT EXISTS audit(id bigserial PRIMARY KEY,workspace_id text NOT NULL,actor text NOT NULL,action text NOT NULL,resource text NOT NULL,ip text NOT NULL,created_at timestamptz NOT NULL DEFAULT now());
`)
	if err != nil {
		return err
	}
	return tx.Commit()
}
func (d *DB) Bootstrap(ctx context.Context, w, email, password string) error {
	if !storage.ValidName(w) || len(password) < 16 || email == "" {
		return errors.New("valid workspace, email and >=16 character bootstrap password required")
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), 12)
	if err != nil {
		return err
	}
	tx, err := d.SQL.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `INSERT INTO workspaces(id) VALUES($1) ON CONFLICT DO NOTHING`, w); err != nil {
		return err
	}
	id := storage.ID()
	if err = tx.QueryRowContext(ctx, `INSERT INTO users(id,email,password_hash) VALUES($1,$2,$3) ON CONFLICT(email) DO UPDATE SET email=EXCLUDED.email RETURNING id`, id, email, string(hash)).Scan(&id); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO memberships VALUES($1,$2,'owner') ON CONFLICT DO NOTHING`, id, w); err != nil {
		return err
	}
	return tx.Commit()
}
func (d *DB) Load() ([]groups.State, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	rows, err := d.SQL.QueryContext(ctx, `SELECT state FROM consumer_groups`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []groups.State{}
	for rows.Next() {
		var raw []byte
		var s groups.State
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
func (d *DB) Save(s groups.State) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	raw, err := json.Marshal(s)
	if err != nil {
		return err
	}
	_, err = d.SQL.ExecContext(ctx, `INSERT INTO consumer_groups VALUES($1,$2,$3,$4) ON CONFLICT(workspace_id,topic,name) DO UPDATE SET state=EXCLUDED.state`, s.Workspace, s.Topic, s.Name, raw)
	return err
}
func (d *DB) Login(ctx context.Context, email, password, w string) (token, csrf string, err error) {
	var id, hash string
	err = d.SQL.QueryRowContext(ctx, `SELECT u.id,u.password_hash FROM users u JOIN memberships m ON m.user_id=u.id WHERE u.email=$1 AND m.workspace_id=$2`, email, w).Scan(&id, &hash)
	if err != nil {
		return "", "", errors.New("invalid credentials")
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(password)) != nil {
		return "", "", errors.New("invalid credentials")
	}
	token = storage.ID() + storage.ID()
	csrf = storage.ID()
	_, err = d.SQL.ExecContext(ctx, `INSERT INTO sessions VALUES($1,$2,$3,$4,now()+interval '12 hours')`, Digest(token), id, w, csrf)
	return
}
func (d *DB) Authenticate(ctx context.Context, token string, cookie bool) (Principal, error) {
	p := Principal{Cookie: cookie}
	if cookie {
		err := d.SQL.QueryRowContext(ctx, `SELECT s.user_id,s.workspace_id,m.role,s.csrf FROM sessions s JOIN memberships m ON m.user_id=s.user_id AND m.workspace_id=s.workspace_id WHERE token_hash=$1 AND expires_at>now()`, Digest(token)).Scan(&p.ID, &p.Workspace, &p.Role, &p.CSRF)
		return p, err
	}
	var raw []byte
	err := d.SQL.QueryRowContext(ctx, `UPDATE api_keys SET last_used_at=now() WHERE token_hash=$1 AND expires_at>now() AND NOT revoked RETURNING id,workspace_id,scopes`, Digest(token)).Scan(&p.ID, &p.Workspace, &raw)
	if err == nil {
		err = json.Unmarshal(raw, &p.Scopes)
	}
	return p, err
}
func (d *DB) Logout(ctx context.Context, token string) error {
	_, err := d.SQL.ExecContext(ctx, `DELETE FROM sessions WHERE token_hash=$1`, Digest(token))
	return err
}
func (d *DB) CreateKey(ctx context.Context, p Principal, name string, scopes []string, expiry time.Time) (string, string, error) {
	token := "ec_" + storage.ID() + storage.ID()
	id := storage.ID()
	raw, err := json.Marshal(scopes)
	if err != nil {
		return "", "", err
	}
	_, err = d.SQL.ExecContext(ctx, `INSERT INTO api_keys(id,workspace_id,token_hash,name,scopes,expires_at) VALUES($1,$2,$3,$4,$5,$6)`, id, p.Workspace, Digest(token), name, raw, expiry)
	return id, token, err
}
func (d *DB) RevokeKey(ctx context.Context, w, id string) error {
	result, err := d.SQL.ExecContext(ctx, `UPDATE api_keys SET revoked=true WHERE id=$1 AND workspace_id=$2`, id, w)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return errors.New("key not found")
	}
	return nil
}
func (d *DB) Audit(ctx context.Context, p Principal, action, resource, ip string) error {
	_, err := d.SQL.ExecContext(ctx, `INSERT INTO audit(workspace_id,actor,action,resource,ip) VALUES($1,$2,$3,$4,$5)`, p.Workspace, p.ID, action, resource, ip)
	return err
}
