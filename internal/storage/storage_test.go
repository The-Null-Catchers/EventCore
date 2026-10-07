package storage

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func fixture(t *testing.T, partitions int) (*Broker, string) {
	t.Helper()
	dir := t.TempDir()
	b, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if err = b.Create(Topic{Workspace: "demo", Name: "orders", Partitions: partitions}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, dir
}
func input(key string) Input {
	return Input{Type: "order.created", Key: key, Data: json.RawMessage(`{"order_id":"123"}`)}
}
func TestDurabilityRotationAndRestart(t *testing.T) {
	b, dir := fixture(t, 4)
	for i := 0; i < 200; i++ {
		if _, err := b.Publish("demo", "orders", input("customer")); err != nil {
			t.Fatal(err)
		}
	}
	idx := PartitionFor("customer", 4)
	before, _ := b.Bounds("demo", "orders", idx)
	if before.Segments < 2 {
		t.Fatal("rotation did not occur")
	}
	b.Close()
	b, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	events, err := b.Read("demo", "orders", idx, 0, 1000)
	if err != nil || len(events) != 200 {
		t.Fatalf("%d %v", len(events), err)
	}
	for i, e := range events {
		if e.Offset != int64(i) || e.Partition != idx {
			t.Fatal(e)
		}
	}
	e, err := b.Publish("demo", "orders", input("customer"))
	if err != nil || e.Offset != 200 {
		t.Fatalf("%+v %v", e, err)
	}
}
func TestConcurrentProducers(t *testing.T) {
	b, _ := fixture(t, 1)
	var wg sync.WaitGroup
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				if _, err := b.Publish("demo", "orders", input("same")); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	events, err := b.Read("demo", "orders", 0, 0, 1000)
	if err != nil || len(events) != 1000 {
		t.Fatalf("%d %v", len(events), err)
	}
	for i, e := range events {
		if e.Offset != int64(i) {
			t.Fatal("offset gap", i, e.Offset)
		}
	}
}
func TestTornTailAndCorruption(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			b, dir := fixture(t, 1)
			b.Publish("demo", "orders", input("a"))
			b.Close()
			paths, _ := filepath.Glob(filepath.Join(dir, "topics/demo/orders/0/*.log"))
			f, err := os.OpenFile(paths[0], os.O_RDWR|os.O_APPEND, 0600)
			if err != nil {
				t.Fatal(err)
			}
			if corrupt {
				f.Close()
				f, _ = os.OpenFile(paths[0], os.O_RDWR, 0600)
				f.WriteAt([]byte{0xff}, 12)
			} else {
				f.Write([]byte{0, 0, 1})
			}
			f.Close()
			b, err = Open(dir, 1024)
			if corrupt {
				if err == nil {
					b.Close()
					t.Fatal("corruption accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer b.Close()
			e, err := b.Publish("demo", "orders", input("a"))
			if err != nil || e.Offset != 1 {
				t.Fatal(e, err)
			}
		})
	}
}
func TestRetentionNeverReusesOffsets(t *testing.T) {
	b, dir := fixture(t, 1)
	b.mu.Lock()
	b.topics["demo/orders"].config.RetentionBytes = 1
	b.mu.Unlock()
	for i := 0; i < 20; i++ {
		b.Publish("demo", "orders", input("a"))
	}
	if err := b.Retain(time.Now()); err != nil {
		t.Fatal(err)
	}
	bounds, _ := b.Bounds("demo", "orders", 0)
	if bounds.Oldest == 0 || bounds.Next != 20 || bounds.Segments != 1 {
		t.Fatal(bounds)
	}
	if _, err := b.Read("demo", "orders", 0, 0, 1); err != ErrRange {
		t.Fatal(err)
	}
	b.Close()
	b, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	e, err := b.Publish("demo", "orders", input("a"))
	if err != nil || e.Offset != 20 {
		t.Fatal(e, err)
	}
}
func TestLockAndValidation(t *testing.T) {
	b, dir := fixture(t, 1)
	if other, err := Open(dir, 1024); err == nil {
		other.Close()
		t.Fatal("second process lock accepted")
	}
	if err := b.Create(Topic{Workspace: "demo", Name: "../escape", Partitions: 1}); err == nil {
		t.Fatal("path traversal accepted")
	}
	if _, err := b.Publish("demo", "orders", Input{Type: "bad", Data: json.RawMessage(`no`)}); err == nil {
		t.Fatal("invalid JSON accepted")
	}
	for i := 0; i < 8; i++ {
		e, err := b.Publish("demo", "orders", input(""))
		if err != nil || e.Offset != int64(i) {
			t.Fatal(e, err)
		}
	}
}
func BenchmarkPublishDurable(b *testing.B) {
	broker, err := Open(b.TempDir(), 4<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer broker.Close()
	broker.Create(Topic{Workspace: "demo", Name: "orders", Partitions: 4})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := broker.Publish("demo", "orders", input("key")); err != nil {
			b.Fatal(err)
		}
	}
}
func TestSchemaEnforcedOnRecovery(t *testing.T) {
	b, dir := fixture(t, 1)
	if err := b.Create(Topic{Workspace: "demo", Name: "strict", Partitions: 1, Schema: json.RawMessage(`{"type":"object","required":["id"],"properties":{"id":{"type":"string"}}}`)}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("demo", "strict", input("")); err == nil {
		t.Fatal("schema bypassed")
	}
	if _, err := b.Publish("demo", "strict", Input{Type: "order", Data: json.RawMessage(`{"id":"123"}`)}); err != nil {
		t.Fatal(err)
	}
	b.Close()
	b, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if _, err = b.Publish("demo", "strict", input("")); err == nil {
		t.Fatal("schema lost on restart")
	}
	if err = b.Create(Topic{Workspace: "demo", Name: "unsafe", Partitions: 1, Schema: json.RawMessage(`{"$ref":"file:///etc/passwd"}`)}); err == nil {
		t.Fatal("external reference accepted")
	}
}
func TestDiskPressureRejectsBeforeAppend(t *testing.T) {
	b, _ := fixture(t, 1)
	usage, err := b.Disk()
	if err != nil {
		t.Fatal(err)
	}
	b.MinFreeBytes = usage.TotalBytes + 1
	if _, err = b.Publish("demo", "orders", input("a")); !errors.Is(err, ErrUnavailable) {
		t.Fatal(err)
	}
	bounds, _ := b.Bounds("demo", "orders", 0)
	if bounds.Next != 0 {
		t.Fatal("disk pressure accepted an append")
	}
	if err = b.Check(); err == nil {
		t.Fatal("disk pressure reported ready")
	}
	b.MinFreeBytes = 0
	if _, err = b.Publish("demo", "orders", input("a")); err != nil {
		t.Fatal(err)
	}
}

func TestCrashRecoverySubprocess(t *testing.T) {
	if dir := os.Getenv("EVENTCORE_CRASH_TEST_DIR"); dir != "" {
		b, err := Open(dir, 1024)
		if err != nil {
			t.Fatal(err)
		}
		if err = b.Create(Topic{Workspace: "demo", Name: "orders", Partitions: 1}); err != nil {
			t.Fatal(err)
		}
		in := input("same")
		in.IdempotencyKey = "crash-request"
		if _, err = b.Publish("demo", "orders", in); err != nil {
			t.Fatal(err)
		}
		fmt.Println("DURABLE")
		for {
			time.Sleep(time.Second)
		}
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestCrashRecoverySubprocess$")
	cmd.Env = append(os.Environ(), "EVENTCORE_CRASH_TEST_DIR="+dir)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { cmd.Process.Kill(); cmd.Wait() }()
	scanner := bufio.NewScanner(stdout)
	if !scanner.Scan() || scanner.Text() != "DURABLE" {
		t.Fatal("child never acknowledged durable event", scanner.Err())
	}
	if err = cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	cmd.Wait()
	broker, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	events, err := broker.Read("demo", "orders", 0, 0, 10)
	if err != nil || len(events) != 1 || events[0].Offset != 0 {
		t.Fatal(events, err)
	}
	retry := input("same")
	retry.IdempotencyKey = "crash-request"
	duplicate, err := broker.Publish("demo", "orders", retry)
	if err != nil || !duplicate.Deduplicated || duplicate.ID != events[0].ID {
		t.Fatal(duplicate, err)
	}
	next, err := broker.Publish("demo", "orders", input("same"))
	if err != nil || next.Offset != 1 {
		t.Fatal(next, err)
	}
}
