package storage

import (
	"errors"
	"testing"
	"time"
)

func TestReplayScanBoundsFiltersAndRecovery(t *testing.T) {
	b, dir := fixture(t, 1)
	var events []Event
	for i := 0; i < 8; i++ {
		e, err := b.Publish("demo", "orders", input("customer"))
		if err != nil {
			t.Fatal(err)
		}
		events = append(events, e)
	}
	end := int64(6)
	from, until := events[2].Timestamp, events[5].Timestamp
	q := Range{Partition: 0, Offset: 0, EndOffset: &end, Limit: 2, FromTime: &from, UntilTime: &until}
	var ids []string
	for {
		page, err := b.Scan("demo", "orders", q)
		if err != nil {
			t.Fatal(err)
		}
		if page.Scanned > 2 || page.EndOffset != 6 || page.NextOffset <= q.Offset {
			t.Fatal(page)
		}
		for _, e := range page.Events {
			ids = append(ids, e.ID)
		}
		if page.Done {
			break
		}
		q.Offset = page.NextOffset
	}
	if len(ids) != 3 || ids[0] != events[2].ID || ids[2] != events[4].ID {
		t.Fatal(ids)
	}
	page, err := b.Scan("demo", "orders", Range{Partition: 0, Offset: 0, Limit: 8, ID: events[4].ID})
	if err != nil || len(page.Events) != 1 || page.Events[0].ID != events[4].ID {
		t.Fatal(page, err)
	}
	page, err = b.Scan("demo", "orders", Range{Partition: 0, Offset: 0, Limit: 2, Type: "missing"})
	if err != nil || len(page.Events) != 0 || page.Scanned != 2 || page.NextOffset != 2 || page.Done {
		t.Fatal(page, err)
	}
	// Snapshot end excludes later appends and survives restart as a caller cursor.
	b.Publish("demo", "orders", input("customer"))
	b.Close()
	recovered, err := Open(dir, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer recovered.Close()
	page, err = recovered.Scan("demo", "orders", Range{Partition: 0, Offset: 6, EndOffset: &end, Limit: 100})
	if err != nil || !page.Done || len(page.Events) != 0 {
		t.Fatal(page, err)
	}
	for _, q := range []Range{{Partition: 0, Offset: -1, Limit: 1}, {Partition: 0, Offset: 10, Limit: 1}, {Partition: 0, Offset: 7, EndOffset: &end, Limit: 1}} {
		if _, err = recovered.Scan("demo", "orders", q); !errors.Is(err, ErrRange) {
			t.Fatal(q, err)
		}
	}
	badUntil := from.Add(-time.Second)
	if _, err = recovered.Scan("demo", "orders", Range{Partition: 0, Offset: 0, Limit: 1, FromTime: &from, UntilTime: &badUntil}); err == nil {
		t.Fatal("invalid timestamps accepted")
	}
}
