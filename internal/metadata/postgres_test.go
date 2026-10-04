package metadata

import (
	"context"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"os"
	"testing"
	"time"
)

func TestPostgresIntegration(t *testing.T) {
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Skip("TEST_DATABASE_URL required for real PostgreSQL integration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer db.SQL.Close()
	if err = db.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	w := "test-" + storage.ID()
	email := w + "@example.com"
	if err = db.Bootstrap(ctx, w, email, "a-long-test-password-1234"); err != nil {
		t.Fatal(err)
	}
	token, csrf, err := db.Login(ctx, email, "a-long-test-password-1234", w)
	if err != nil {
		t.Fatal(err)
	}
	p, err := db.Authenticate(ctx, token, true)
	if err != nil || p.Workspace != w || p.Role != "owner" || p.CSRF != csrf {
		t.Fatal(p, err)
	}
	id, key, err := db.CreateKey(ctx, p, "producer", []string{"topic:orders:produce"}, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	api, err := db.Authenticate(ctx, key, false)
	if err != nil || api.Workspace != w || api.Scopes[0] != "topic:orders:produce" {
		t.Fatal(api, err)
	}
	if err = db.RevokeKey(ctx, w, id); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Authenticate(ctx, key, false); err == nil {
		t.Fatal("revoked key authenticated")
	}
	state := groups.State{Workspace: w, Topic: "orders", Name: "billing", Epoch: 3, Offsets: map[int]int64{0: 999}}
	if err = db.Save(state); err != nil {
		t.Fatal(err)
	}
	states, err := db.Load()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, s := range states {
		if s.Workspace == w {
			found = s.Offsets[0] == 999 && s.Epoch == 3
		}
	}
	if !found {
		t.Fatal("commit not restored")
	}
	if err = db.Logout(ctx, token); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Authenticate(ctx, token, true); err == nil {
		t.Fatal("logged-out session authenticated")
	}
}
