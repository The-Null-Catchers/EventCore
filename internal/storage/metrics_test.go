package storage

import (
	"testing"
)

func TestMetricsCountOnlyActualAppendsAndResetOnRecovery(t *testing.T) {
	b, dir := fixture(t, 1)
	in := input("same")
	in.IdempotencyKey = "request"
	e, err := b.Publish("demo", "orders", in)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Publish("demo", "orders", in); err != nil {
		t.Fatal(err)
	}
	m, err := b.Metrics("demo")
	if err != nil || len(m) != 1 || m[0].Publish.Events != 1 || m[0].Partitions[0].Next != 1 || m[0].Publish.Bytes != uint64(m[0].Partitions[0].Bytes) {
		t.Fatal(m, err)
	}
	for i := 1; i < len(m[0].Publish.Buckets); i++ {
		if m[0].Publish.Buckets[i] < m[0].Publish.Buckets[i-1] {
			t.Fatal("non-monotonic histogram")
		}
	}
	other, err := b.Metrics("other")
	if err != nil || len(other) != 0 {
		t.Fatal(other, err)
	}
	if _, err = b.Publish("demo", "orders", Input{}); err == nil {
		t.Fatal("invalid accepted")
	}
	b.Close()
	recovered, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	m, err = recovered.Metrics("demo")
	if err != nil || m[0].Publish.Events != 0 || m[0].Partitions[0].Next != 1 {
		t.Fatal(m, err)
	}
	e2, err := recovered.Publish("demo", "orders", in)
	if err != nil || !e2.Deduplicated || e2.ID != e.ID {
		t.Fatal(e2, err)
	}
	m, _ = recovered.Metrics("demo")
	if m[0].Publish.Events != 0 {
		t.Fatal("recovered receipt counted as append")
	}
}
