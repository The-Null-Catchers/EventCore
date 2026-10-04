// Package webhooks delivers durable group events with signed requests and bounded retries.
package webhooks

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/The-Null-Catchers/EventCore/internal/groups"
	"github.com/The-Null-Catchers/EventCore/internal/storage"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Subscription struct {
	ID              string `json:"id"`
	Workspace       string `json:"workspace"`
	Topic           string `json:"topic"`
	URL             string `json:"url"`
	Secret          string `json:"secret,omitempty"`
	EncryptedSecret string `json:"-"`
	MaxAttempts     int    `json:"max_attempts"`
	DelaySeconds    int    `json:"delay_seconds"`
	Paused          bool   `json:"paused"`
}
type Attempt struct {
	Count      int       `json:"attempts"`
	Next       time.Time `json:"next_attempt_at"`
	Status     string    `json:"status"`
	HTTPStatus int       `json:"http_status"`
	LatencyMS  int64     `json:"latency_ms"`
	Error      string    `json:"error,omitempty"`
}
type Store interface {
	Subscriptions(context.Context) ([]Subscription, error)
	Create(context.Context, Subscription) error
	Attempt(context.Context, string, string) (Attempt, error)
	SaveAttempt(context.Context, string, string, Attempt) error
}
type Worker struct {
	Broker *storage.Broker
	Groups *groups.Coordinator
	Store  Store
	Key    []byte
	Client *http.Client
	Now    func() time.Time
}

func Encrypt(key []byte, secret string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err = rand.Read(nonce); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(aead.Seal(nonce, nonce, []byte(secret), []byte("eventcore.webhook.v1"))), nil
}
func Decrypt(key []byte, encrypted string) (string, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return "", err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	raw, err := base64.StdEncoding.DecodeString(encrypted)
	if err != nil || len(raw) < aead.NonceSize() {
		return "", errors.New("invalid encrypted secret")
	}
	nonce := raw[:aead.NonceSize()]
	plain, err := aead.Open(nil, nonce, raw[aead.NonceSize():], []byte("eventcore.webhook.v1"))
	return string(plain), err
}

// Deny nonpublic, special-purpose and metadata ranges, including IPv4-mapped IPv6.
var denied = func() []netip.Prefix {
	out := []netip.Prefix{}
	for _, s := range []string{"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12", "192.0.0.0/24", "192.0.2.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4", "::/96", "::1/128", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2002::/16", "fc00::/7", "fe80::/10", "ff00::/8"} {
		out = append(out, netip.MustParsePrefix(s))
	}
	return out
}()

func PublicIP(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() {
		return false
	}
	for _, p := range denied {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}
func ValidateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || len(raw) > 2048 {
		return errors.New("webhook must be an HTTPS URL without credentials or fragment")
	}
	if port := u.Port(); port != "" && port != "443" {
		return errors.New("only HTTPS port 443 supported")
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && !PublicIP(ip) {
		return errors.New("webhook destination is not public")
	}
	return nil
}
func SafeClient() *http.Client {
	transport := &http.Transport{Proxy: nil, MaxIdleConns: 16, MaxConnsPerHost: 4, IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, errors.New("no DNS addresses")
		}
		for _, ip := range ips {
			if !PublicIP(ip) {
				return nil, errors.New("DNS resolved to a prohibited address")
			}
		}
		dialer := net.Dialer{Timeout: 5 * time.Second}
		var last error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
		}
		return nil, last
	}}
	return &http.Client{Timeout: 10 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("webhook redirects prohibited") }}
}
func Sign(secret, timestamp string, payload []byte) string {
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(timestamp + "."))
	h.Write(payload)
	return "v1=" + hex.EncodeToString(h.Sum(nil))
}
func (w *Worker) Initialize(ctx context.Context, s Subscription) error {
	if err := ValidateURL(s.URL); err != nil {
		return err
	}
	if len(s.Secret) < 32 || s.MaxAttempts < 1 || s.MaxAttempts > 20 || s.DelaySeconds < 1 || s.DelaySeconds > 3600 {
		return errors.New("secret >=32 characters, attempts 1..20, delay 1..3600 required")
	}
	if len(w.Key) != 32 {
		return errors.New("webhook encryption key must be 32 bytes")
	}
	if _, err := w.Broker.Config(s.Workspace, s.Topic); err != nil {
		return err
	}
	s.ID = storage.ID()
	var err error
	s.EncryptedSecret, err = Encrypt(w.Key, s.Secret)
	if err != nil {
		return err
	}
	s.Secret = ""
	dlq := s.Topic + ".DLQ"
	if len(dlq) > 128 {
		return errors.New("topic name too long for DLQ")
	}
	if config, existsErr := w.Broker.Config(s.Workspace, dlq); existsErr == nil && (config.Partitions != 1 || len(config.Schema) > 0 || config.MaxEventBytes < (2<<20)-4096) {
		return errors.New("existing DLQ topic is incompatible")
	}
	if _, err = w.Broker.Config(s.Workspace, dlq); err != nil {
		if err = w.Broker.Create(storage.Topic{Workspace: s.Workspace, Name: dlq, Partitions: 1, MaxEventBytes: (2 << 20) - 4096}); err != nil {
			return err
		}
	}
	if err = w.Groups.Create(s.Workspace, s.Topic, "webhook-"+s.ID, "earliest"); err != nil {
		return err
	}
	return w.Store.Create(ctx, s)
}
func (w *Worker) Tick(ctx context.Context) error {
	if w.Client == nil {
		w.Client = SafeClient()
	}
	if w.Now == nil {
		w.Now = time.Now
	}
	subs, err := w.Store.Subscriptions(ctx)
	if err != nil {
		return err
	}
	for _, s := range subs {
		if s.Paused {
			continue
		}
		if err = w.deliver(ctx, s); err != nil {
			slog.Error("webhook processing failed", "subscription", s.ID, "error", err)
		}
	}
	return nil
}
func (w *Worker) deliver(ctx context.Context, s Subscription) error {
	group := "webhook-" + s.ID
	member := "worker-" + s.ID
	snap, err := w.Groups.Join(s.Workspace, s.Topic, group, member)
	if err != nil {
		return err
	}
	batches, err := w.Groups.Pull(s.Workspace, s.Topic, group, member, snap.Epoch, 1)
	if err != nil {
		return err
	}
	for _, batch := range batches {
		event := batch.Events[0]
		attempt, err := w.Store.Attempt(ctx, s.ID, event.ID)
		if err != nil {
			return err
		}
		settle := func(nack bool) error {
			return w.Groups.Ack(s.Workspace, s.Topic, group, member, batch.Epoch, batch.Partition, batch.Token, nack)
		}
		if attempt.Status == "delivered" || attempt.Status == "dlq" {
			if err = settle(false); err != nil {
				return err
			}
			continue
		}
		if attempt.Next.After(w.Now()) {
			if err = settle(true); err != nil {
				return err
			}
			continue
		}
		if attempt.Count < s.MaxAttempts {
			attempt.Count++
			attempt.Status = "sending"
			attempt.Next = w.Now().Add(30 * time.Second)
			if err = w.Store.SaveAttempt(ctx, s.ID, event.ID, attempt); err != nil {
				return err
			}
			payload, err := json.Marshal(event)
			if err != nil {
				return err
			}
			secret, err := Decrypt(w.Key, s.EncryptedSecret)
			if err != nil {
				return err
			}
			if err = ValidateURL(s.URL); err != nil {
				return err
			}
			req, err := http.NewRequestWithContext(ctx, "POST", s.URL, bytes.NewReader(payload))
			if err != nil {
				return err
			}
			ts := strconv.FormatInt(w.Now().Unix(), 10)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("X-EventCore-ID", event.ID)
			req.Header.Set("X-EventCore-Timestamp", ts)
			req.Header.Set("X-EventCore-Signature", Sign(secret, ts, payload))
			start := time.Now()
			resp, sendErr := w.Client.Do(req)
			attempt.LatencyMS = time.Since(start).Milliseconds()
			attempt.HTTPStatus = 0
			if resp != nil {
				attempt.HTTPStatus = resp.StatusCode
				io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
				resp.Body.Close()
			}
			attempt.Error = ""
			if sendErr != nil {
				attempt.Error = "transport failure"
			} else if attempt.HTTPStatus < 200 || attempt.HTTPStatus >= 300 {
				attempt.Error = fmt.Sprintf("HTTP %d", attempt.HTTPStatus)
			}
			if attempt.Error == "" {
				attempt.Status = "delivered"
				if err = w.Store.SaveAttempt(ctx, s.ID, event.ID, attempt); err != nil {
					return err
				}
				if err = settle(false); err != nil {
					return err
				}
				continue
			}
			attempt.Status = "retry"
			delay := time.Duration(s.DelaySeconds) * time.Second
			for n := 1; n < attempt.Count && delay < time.Hour; n++ {
				delay *= 2
			}
			if delay > time.Hour {
				delay = time.Hour
			}
			attempt.Next = w.Now().Add(delay)
			if err = w.Store.SaveAttempt(ctx, s.ID, event.ID, attempt); err != nil {
				return err
			}
		}
		if attempt.Count >= s.MaxAttempts {
			raw, err := json.Marshal(map[string]any{"original_event": event, "original_topic": s.Topic, "original_partition": event.Partition, "original_offset": event.Offset, "subscription_id": s.ID, "error": attempt.Error, "attempts": attempt.Count, "failed_at": w.Now()})
			if err != nil {
				return err
			}
			if _, err = w.Broker.Publish(s.Workspace, s.Topic+".DLQ", storage.Input{Type: "eventcore.delivery.failed", Key: event.ID, Data: raw}); err != nil {
				return err
			}
			attempt.Status = "dlq"
			if err = w.Store.SaveAttempt(ctx, s.ID, event.ID, attempt); err != nil {
				return err
			}
			if err = settle(false); err != nil {
				return err
			}
		} else {
			if err = settle(true); err != nil {
				return err
			}
		}
	}
	return nil
}
func ParseKey(s string) ([]byte, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil || len(raw) != 32 {
		return nil, errors.New("WEBHOOK_ENCRYPTION_KEY must be base64 encoding of 32 bytes")
	}
	return raw, nil
}
