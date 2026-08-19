package delivery

import (
	"context"
	"time"

	"webhookgw/internal/model"
)

// Report is the operator-facing state snapshot for the delivery pipeline.
// It combines durable queue state with process-local delivery counters so an
// operator can distinguish work waiting in SQLite from work already observed
// by the running dispatcher.
type Report struct {
	Subscriptions     int        `json:"subscriptions"`
	Events            int        `json:"events"`
	QueueTotal        int        `json:"queue_total"`
	Pending           int        `json:"pending"`
	DurableInFlight   int        `json:"durable_in_flight"`
	DeliveredAttempts int        `json:"delivered_attempts"`
	FailedAttempts    int        `json:"failed_attempts"`
	DeadLetters       int        `json:"dead_letters"`
	OldestDueAt       *time.Time `json:"oldest_due_at,omitempty"`
	LatestUpdated     *time.Time `json:"latest_updated_at,omitempty"`
	Stalled           bool       `json:"stalled"`
	RecoveryRequired  bool       `json:"recovery_required"`
	PendingRatio      float64    `json:"pending_ratio"`
	StallAgeSeconds   int64      `json:"stall_age_seconds"`
	Active            bool       `json:"active"`
	Empty             bool       `json:"empty"`
	HasHistory        bool       `json:"has_history"`
	Delivered         int64      `json:"delivered"`
	Failed            int64      `json:"failed"`
	InFlight          int64      `json:"in_flight"`
}

// Report reads the durable state and current metrics without mutating the
// queue. The context is checked before the database work so a cancelled
// diagnostics request does not start a potentially expensive snapshot.
func (m *Manager) Report(ctx context.Context) (Report, error) {
	if err := ctx.Err(); err != nil {
		return Report{}, err
	}
	durable, err := m.store.Diagnostics()
	if err != nil {
		return Report{}, err
	}
	queue, err := m.store.QueueSummary()
	if err != nil {
		return Report{}, err
	}
	total, _ := m.metrics.Snapshot()
	return Report{
		Subscriptions:     durable.Subscriptions,
		Events:            durable.Events,
		QueueTotal:        queue.Total,
		Pending:           queue.Pending,
		DurableInFlight:   queue.InFlight,
		DeliveredAttempts: queue.Delivered,
		FailedAttempts:    queue.Failed,
		DeadLetters:       queue.DeadLetters,
		OldestDueAt:       queue.OldestDueAt,
		LatestUpdated:     queue.LatestUpdated,
		Stalled:           queue.IsStalled(m.clk.Now(), 5*time.Minute),
		RecoveryRequired:  queue.RecoveryRequired(),
		PendingRatio:      queue.PendingRatio(),
		StallAgeSeconds:   int64(queue.StallAge(m.clk.Now()).Seconds()),
		Active:            queue.Active(),
		Empty:             queue.IsEmpty(),
		HasHistory:        queue.HasHistory(),
		Delivered:         total.Delivered,
		Failed:            total.Failed,
		InFlight:          total.InFlight,
	}, nil
}

// AttemptReady reports whether an attempt can be selected by a dispatch pass.
func AttemptReady(a *model.Attempt, now time.Time) bool {
	return a != nil && a.Status == model.StatusPending && !a.NextAttemptAt.After(now)
}
