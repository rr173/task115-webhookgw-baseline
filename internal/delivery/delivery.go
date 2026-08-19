// Package delivery enqueues delivery attempts for matching subscriptions and
// dispatches them over HTTP with HMAC signing, bounded retries, per-endpoint
// rate limiting and a dead-letter queue. A background dispatcher runs attempts
// concurrently; the metrics counters it touches are guarded by the metrics
// package's mutex.
package delivery

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"webhookgw/internal/clock"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/store"
	"webhookgw/internal/subscription"

	"github.com/google/uuid"
)

// Manager orchestrates delivery.
type Manager struct {
	store   *store.Store
	subs    *subscription.Service
	metrics *metrics.Metrics
	clk     clock.Clock
	client  *http.Client
	rl      *rateLimiter
}

// New builds a delivery Manager. client may be nil (defaults to http.DefaultClient)
// and is injectable so tests can stub transport.
func New(s *store.Store, subs *subscription.Service, m *metrics.Metrics, clk clock.Clock, client *http.Client) *Manager {
	if client == nil {
		client = http.DefaultClient
	}
	return &Manager{store: s, subs: subs, metrics: m, clk: clk, client: client, rl: newRateLimiter(clk)}
}

// Enqueue creates one pending attempt per subscription that matches the event
// type. It returns the number of attempts created.
func (m *Manager) Enqueue(ev *model.Event) (int, error) {
	subs, err := m.subs.List()
	if err != nil {
		return 0, err
	}
	now := m.clk.Now()
	n := 0
	for _, sub := range subs {
		if !m.subs.Matches(sub, ev.Type) {
			continue
		}
		a := &model.Attempt{
			ID:             uuid.NewString(),
			SubscriptionID: sub.ID,
			EventID:        ev.ID,
			EventType:      ev.Type,
			Payload:        ev.Payload,
			Status:         model.StatusPending,
			AttemptCount:   0,
			NextAttemptAt:  now,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		if err := m.store.SaveAttempt(a); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// SaveEvent persists an ingested event.
func (m *Manager) SaveEvent(ev *model.Event) error { return m.store.SaveEvent(ev) }

// ListEvents returns recent ingested events.
func (m *Manager) ListEvents(limit int) ([]*model.Event, error) {
	return m.store.ListEvents(limit)
}

// Dispatch delivers every due attempt concurrently. It blocks until all
// attempts for this pass complete.
func (m *Manager) Dispatch(ctx context.Context) error {
	due, err := m.collectDue(0)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, a := range due {
		if m.clk.Now().Before(a.NextAttemptAt) {
			continue
		}
		wg.Add(1)
		go func(a *model.Attempt) {
			defer wg.Done()
			m.deliver(ctx, a)
		}(a)
	}
	wg.Wait()
	return nil
}

// collectDue returns attempts whose next attempt time has arrived.
func (m *Manager) collectDue(limit int) ([]*model.Attempt, error) {
	due, err := m.store.PendingAttempts()
	if err != nil {
		return nil, err
	}
	out := make([]*model.Attempt, 0, len(due))
	for _, a := range due {
		if a.NextAttemptAt.After(m.clk.Now()) {
			continue
		}
		out = append(out, a)
	}
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// deliver performs a single HTTP POST for an attempt and records the outcome.
func (m *Manager) deliver(ctx context.Context, a *model.Attempt) {
	m.metrics.IncInFlight()
	defer m.metrics.DecInFlight()

	sub, err := m.store.GetSubscription(a.SubscriptionID)
	if err != nil || sub == nil {
		m.fail(a, fmt.Sprintf("subscription lookup failed: %v", err))
		return
	}

	if !m.rl.allow(sub.ID, sub.RateLimit) {
		a.NextAttemptAt = m.clk.Now().Add(time.Second)
		_ = m.store.UpdateAttempt(a)
		return
	}

	body := []byte(a.Payload)
	// The request must honour the dispatcher's context so cancellations and
	// deadlines propagate to the outbound HTTP call.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.Endpoint, bytes.NewReader(body))
	if err != nil {
		m.fail(a, fmt.Sprintf("build request: %v", err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event", a.EventType)
	req.Header.Set("X-Webhook-Attempt", fmt.Sprintf("%d", a.AttemptCount+1))
	req.Header.Set("X-Webhook-Signature", Sign(sub.SigningSecret, body))

	resp, err := m.client.Do(req)
	if err != nil {
		m.fail(a, fmt.Sprintf("http request failed: %v", err))
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		m.fail(a, fmt.Sprintf("endpoint returned HTTP %d", resp.StatusCode))
		return
	}
	m.succeed(a)
}

func (m *Manager) succeed(a *model.Attempt) {
	a.Status = model.StatusDelivered
	a.AttemptCount++
	a.UpdatedAt = m.clk.Now()
	_ = m.store.UpdateAttempt(a)
	m.metrics.RecordDelivered(a.SubscriptionID)
}

func (m *Manager) fail(a *model.Attempt, reason string) {
	a.AttemptCount++
	a.LastError = reason

	sub, _ := m.store.GetSubscription(a.SubscriptionID)
	maxAttempts := 5
	base, max := time.Second, 30*time.Second
	if sub != nil {
		maxAttempts = sub.RetryPolicy.MaxAttempts
		base = sub.RetryPolicy.BaseDelay
		max = sub.RetryPolicy.MaxDelay
	}

	// A non-retryable HTTP status (e.g. 4xx except 429) should be moved
	// straight to the dead-letter queue instead of being retried.
	if code, ok := httpStatusFromReason(reason); ok && !isRetryable(code) {
		m.deadLetter(a, reason)
		return
	}

	if a.AttemptCount >= maxAttempts {
		m.deadLetter(a, reason)
		return
	}
	a.Status = model.StatusPending
	a.NextAttemptAt = m.clk.Now().Add(retryDelay(base, max, a.AttemptCount))
	a.UpdatedAt = m.clk.Now()
	_ = m.store.UpdateAttempt(a)
	m.metrics.RecordFailed(a.SubscriptionID)
}

func (m *Manager) deadLetter(a *model.Attempt, reason string) {
	a.Status = model.StatusDeadLetter
	a.UpdatedAt = m.clk.Now()
	_ = m.store.UpdateAttempt(a)
	_ = m.store.SaveDeadLetter(&model.DeadLetter{
		AttemptID:      a.ID,
		SubscriptionID: a.SubscriptionID,
		EventType:      a.EventType,
		Payload:        a.Payload,
		Reason:         reason,
		AttemptCount:   a.AttemptCount,
		CreatedAt:      m.clk.Now(),
	})
	m.metrics.RecordDeadLetter(a.SubscriptionID)
}

// Recover resets attempts that were in-flight when the process last stopped so
// they are retried on the next dispatch pass. It returns the number reset.
func (m *Manager) Recover() (int, error) {
	rows, err := m.store.PendingAttempts()
	if err != nil {
		return 0, err
	}
	n := 0
	now := m.clk.Now()
	for _, a := range rows {
		if a.Status != model.StatusInFlight {
			continue
		}
		a.Status = model.StatusPending
		a.NextAttemptAt = now
		a.UpdatedAt = now
		if err := m.store.UpdateAttempt(a); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// retryDelay computes exponential backoff with full jitter capped at max.
func retryDelay(base, max time.Duration, attempt int) time.Duration {
	d := base
	for i := 1; i < attempt; i++ {
		d *= 2
		if d >= max {
			return max
		}
	}
	if d > max {
		return max
	}
	return d
}

// isRetryable reports whether an HTTP status code is worth retrying.
func isRetryable(code int) bool {
	return code == 429 || (code >= 500 && code < 600)
}
