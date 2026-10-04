package main

import (
	"context"
	"errors"
	"github.com/The-Null-Catchers/EventCore/internal/api"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func env(k, defaultValue string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return defaultValue
}
func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	startup, done := context.WithTimeout(ctx, 15*time.Second)
	defer done()
	db, err := metadata.Open(startup, os.Getenv("DATABASE_URL"))
	if err != nil {
		return err
	}
	defer db.SQL.Close()
	if err = db.Migrate(startup); err != nil {
		return err
	}
	leadership, err := db.AcquireBroker(startup)
	if err != nil {
		return err
	}
	defer db.ReleaseBroker(leadership)
	if password := os.Getenv("BOOTSTRAP_PASSWORD"); password != "" {
		if err = db.Bootstrap(startup, env("BOOTSTRAP_WORKSPACE", "demo"), env("BOOTSTRAP_EMAIL", "owner@example.com"), password); err != nil {
			return err
		}
	}
	dataPath := env("BROKER_DATA_PATH", "data")
	broker, err := storage.Open(dataPath, 4<<20)
	if err != nil {
		return err
	}
	defer broker.Close()
	if raw := os.Getenv("BROKER_MIN_FREE_BYTES"); raw != "" {
		floor, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || floor < 0 {
			return errors.New("BROKER_MIN_FREE_BYTES must be nonnegative")
		}
		broker.MinFreeBytes = floor
	}
	coordinator, err := groups.New(broker, db, 30*time.Second, 30*time.Second)
	if err != nil {
		return err
	}
	secure, err := strconv.ParseBool(env("COOKIE_SECURE", "true"))
	if err != nil {
		return err
	}
	var webhookWorker *webhooks.Worker
	if raw := os.Getenv("WEBHOOK_ENCRYPTION_KEY"); raw != "" {
		key, err := webhooks.ParseKey(raw)
		if err != nil {
			return err
		}
		webhookWorker = &webhooks.Worker{Broker: broker, Groups: coordinator, Store: db, Key: key}
		workerDone := make(chan struct{})
		defer func() { cancel(); <-workerDone }()
		go func() {
			defer close(workerDone)
			tick := time.NewTicker(time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if err := webhookWorker.Tick(ctx); err != nil {
						slog.Error("webhook worker failed", "error", err)
					}
				}
			}
		}()
	}
	server := &api.Server{Webhooks: webhookWorker, WebhookAdmin: db, Broker: broker, Groups: coordinator, Auth: db, SecureCookies: secure, Ready: func(ctx context.Context) error {
		if err := broker.Check(); err != nil {
			return err
		}
		if err := db.SQL.PingContext(ctx); err != nil {
			return err
		}
		f, err := os.CreateTemp(dataPath, ".health-")
		if err != nil {
			return err
		}
		defer os.Remove(f.Name())
		defer f.Close()
		if _, err = f.Write([]byte("probe")); err != nil {
			return err
		}
		return f.Sync()
	}}
	httpServer := &http.Server{Addr: env("HTTP_ADDR", ":8080"), Handler: server.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	errs := make(chan error, 1)
	go func() {
		slog.Info("api starting", "address", httpServer.Addr, "version", "0.1.0-dev", "replication", false)
		errs <- httpServer.ListenAndServe()
	}()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
			defer stop()
			return httpServer.Shutdown(shutdown)
		case err := <-errs:
			if errors.Is(err, http.ErrServerClosed) {
				return nil
			}
			return err
		case now := <-ticker.C:
			if err := broker.Retain(now); err != nil {
				slog.Error("retention failed", "error", err)
			}
		}
	}
}
func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		client := http.Client{Timeout: 2 * time.Second}
		resp, err := client.Get("http://127.0.0.1:8080/ready")
		if err != nil {
			os.Exit(1)
		}
		resp.Body.Close()
		if resp.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}

	if err := run(); err != nil {
		slog.Error("startup or serving failed", "error", err)
		os.Exit(1)
	}
}
