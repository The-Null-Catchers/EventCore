package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/The-Null-Catchers/EventCore/internal/metadata"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"github.com/The-Null-Catchers/EventCore/internal/webhooks"
	"net/http"
	"strconv"
	"time"
)

type MetadataMetrics interface {
	WebhookMetrics(context.Context, string) (webhooks.Metrics, error)
}

func (s *Server) metrics(w http.ResponseWriter, r *http.Request, p metadata.Principal) {
	// Collect before emitting headers: a dependency error must not become a
	// successful, silently incomplete scrape.
	if s.Broker == nil || s.Groups == nil {
		s.internal(w, errors.New("broker/coordinator metrics unavailable"))
		return
	}
	topics, err := s.Broker.Metrics(p.Workspace)
	if err != nil {
		s.internal(w, err)
		return
	}
	groups, err := s.Groups.Metrics(p.Workspace)
	if err != nil {
		s.internal(w, err)
		return
	}
	disk, err := s.Broker.Disk()
	if err != nil {
		s.internal(w, err)
		return
	}
	var deliveries webhooks.Metrics
	if s.MetadataMetrics != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		deliveries, err = s.MetadataMetrics.WebhookMetrics(ctx, p.Workspace)
		cancel()
		if err != nil {
			s.internal(w, err)
			return
		}
	}
	var out bytes.Buffer
	metric := func(name, kind, help string) {
		fmt.Fprintf(&out, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
	}
	published, consumed, bytesWritten, deadLetters := uint64(0), uint64(0), uint64(0), uint64(0)
	seconds := 0.0
	var buckets [8]uint64
	partitions, members := 0, 0
	for _, topic := range topics {
		published += topic.Publish.Events
		bytesWritten += topic.Publish.Bytes
		deadLetters += topic.Publish.DeadLetters
		seconds += topic.Publish.Seconds
		partitions += len(topic.Partitions)
		for i, v := range topic.Publish.Buckets {
			buckets[i] += v
		}
	}
	for _, g := range groups {
		consumed += g.Consumed
		members += g.Members
	}
	metric("events_published_total", "counter", "Durable broker appends in this workspace since process start; excludes deduplicated receipts.")
	fmt.Fprintf(&out, "events_published_total %d\n", published)
	metric("events_consumed_total", "counter", "Events returned by successful group pulls since process start; includes redelivery and webhook pulls.")
	fmt.Fprintf(&out, "events_consumed_total %d\n", consumed)
	metric("bytes_published_total", "counter", "Encoded log bytes appended in this workspace since process start.")
	fmt.Fprintf(&out, "bytes_published_total %d\n", bytesWritten)
	metric("dlq_events_total", "counter", "Dead-letter envelopes actually appended to DLQ logs since process start.")
	fmt.Fprintf(&out, "dlq_events_total %d\n", deadLetters)
	metric("publish_latency_seconds", "histogram", "Successful durable append latency including validation, locking, preparation and fsync.")
	for i, b := range storage.PublishLatencyBuckets() {
		fmt.Fprintf(&out, "publish_latency_seconds_bucket{le=%q} %d\n", strconv.FormatFloat(b, 'g', -1, 64), buckets[i])
	}
	fmt.Fprintf(&out, "publish_latency_seconds_bucket{le=\"+Inf\"} %d\npublish_latency_seconds_sum %.9f\npublish_latency_seconds_count %d\n", published, seconds, published)
	for _, m := range []struct {
		name, help string
		value      int
	}{{"active_topics", "Topics in the authenticated workspace.", len(topics)}, {"active_partitions", "Partitions in the authenticated workspace.", partitions}, {"active_consumer_groups", "Persisted consumer groups in this workspace.", len(groups)}, {"active_consumers", "Unexpired group memberships; the same member ID in two groups counts twice.", members}} {
		metric(m.name, "gauge", m.help)
		fmt.Fprintf(&out, "%s %d\n", m.name, m.value)
	}
	metric("broker_uptime_seconds", "gauge", "HTTP process uptime.")
	fmt.Fprintf(&out, "broker_uptime_seconds %.3f\n", time.Since(s.started).Seconds())
	metric("broker_disk_bytes", "gauge", "Retained segment bytes by workspace topic and partition.")
	metric("partition_oldest_offset", "gauge", "Oldest retained offset.")
	metric("partition_next_offset", "gauge", "Exclusive partition high watermark.")
	metric("partition_retained_events", "gauge", "Retained records; not cumulative publications.")
	for _, t := range topics {
		for i, b := range t.Partitions {
			labels := fmt.Sprintf("workspace=%q,topic=%q,partition=%q", p.Workspace, t.Topic, strconv.Itoa(i))
			fmt.Fprintf(&out, "broker_disk_bytes{%s} %d\npartition_oldest_offset{%s} %d\npartition_next_offset{%s} %d\npartition_retained_events{%s} %d\n", labels, b.Bytes, labels, b.Oldest, labels, b.Next, labels, b.Events)
		}
	}
	metric("consumer_lag", "gauge", "Distance from committed next offset to high watermark, including expired offsets.")
	metric("consumer_committed_offset", "gauge", "Committed next offset by partition.")
	metric("consumer_unavailable_events", "gauge", "Unprocessed offsets removed by retention; requires explicit reset.")
	metric("consumer_inflight_partition", "gauge", "One while an unexpired lease and membership exist.")
	metric("consumer_group_members", "gauge", "Unexpired memberships by group.")
	for _, g := range groups {
		fmt.Fprintf(&out, "consumer_group_members{workspace=%q,topic=%q,group=%q} %d\n", p.Workspace, g.Topic, g.Group, g.Members)
		for _, v := range g.Partitions {
			labels := fmt.Sprintf("workspace=%q,topic=%q,group=%q,partition=%q", p.Workspace, g.Topic, g.Group, strconv.Itoa(v.Partition))
			inflight := 0
			if v.InFlight {
				inflight = 1
			}
			fmt.Fprintf(&out, "consumer_lag{%s} %d\nconsumer_committed_offset{%s} %d\nconsumer_unavailable_events{%s} %d\nconsumer_inflight_partition{%s} %d\n", labels, v.Lag, labels, v.Committed, labels, v.Unavailable, labels, inflight)
		}
	}
	metric("broker_filesystem_available_bytes", "gauge", "Node-level available filesystem bytes shared by workspaces and other processes.")
	fmt.Fprintf(&out, "broker_filesystem_available_bytes %d\n", disk.AvailableBytes)
	metric("broker_filesystem_capacity_bytes", "gauge", "Node-level filesystem capacity.")
	fmt.Fprintf(&out, "broker_filesystem_capacity_bytes %d\n", disk.TotalBytes)
	metric("broker_storage_pressure", "gauge", "One when available disk is below the configured append admission reserve.")
	pressure := 0
	if disk.AvailableBytes < s.Broker.MinFreeBytes || disk.AvailableBytes-s.Broker.MinFreeBytes < (2<<20) {
		pressure = 1
	}
	fmt.Fprintf(&out, "broker_storage_pressure %d\n", pressure)
	if s.MetadataMetrics != nil {
		metric("webhook_delivery_total", "counter", "Durable recorded attempt outcomes; legacy known states seed the baseline.")
		fmt.Fprintf(&out, "webhook_delivery_total{outcome=\"success\"} %d\nwebhook_delivery_total{outcome=\"failure\"} %d\nwebhook_delivery_total{outcome=\"unknown\"} %d\n", deliveries.Delivered, deliveries.Failed, deliveries.Unknown)
		metric("webhook_failure_total", "counter", "Durable failed attempt outcomes; repeated metadata writes do not increment it.")
		fmt.Fprintf(&out, "webhook_failure_total %d\n", deliveries.Failed)
		metric("webhook_dlq_routes_total", "counter", "Durable completed webhook DLQ transitions; not physical log append count.")
		fmt.Fprintf(&out, "webhook_dlq_routes_total %d\n", deliveries.DeadLetters)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	w.Write(out.Bytes())
}

func allowedMetrics(p metadata.Principal) bool {
	if p.Role == "owner" || p.Role == "admin" {
		return true
	}
	for _, scope := range p.Scopes {
		if scope == "admin" || scope == "metrics:read" {
			return true
		}
	}
	return false
}
