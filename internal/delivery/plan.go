package delivery

import (
	"context"
	"fmt"
	"sort"
	"time"

	"webhookgw/internal/model"
)

// PlanItem explains why an attempt will or will not be selected by the next
// dispatcher pass. It is derived from durable attempt state so operators can
// inspect a queue before triggering a dispatch.
type PlanItem struct {
	AttemptID      string    `json:"attempt_id"`
	SubscriptionID string    `json:"subscription_id"`
	EventType      string    `json:"event_type"`
	Status         string    `json:"status"`
	AttemptCount   int       `json:"attempt_count"`
	MaxAttempts    int       `json:"max_attempts"`
	DueAt          time.Time `json:"due_at"`
	Ready          bool      `json:"ready"`
	WithinWindow   bool      `json:"within_window"`
	Retryable      bool      `json:"retryable"`
	Reason         string    `json:"reason"`
}

// Plan is a bounded preview of the durable delivery queue.
type Plan struct {
	GeneratedAt    time.Time  `json:"generated_at"`
	Window         string     `json:"window"`
	Total          int        `json:"total"`
	Ready          int        `json:"ready"`
	Deferred       int        `json:"deferred"`
	RetryExhausted int        `json:"retry_exhausted"`
	Items          []PlanItem `json:"items"`
}

// Plan builds a deterministic queue preview. A zero horizon means "due now";
// a positive horizon includes attempts due during that interval.
func (m *Manager) Plan(ctx context.Context, horizon time.Duration, limit int) (Plan, error) {
	if err := ctx.Err(); err != nil {
		return Plan{}, err
	}
	if horizon < 0 {
		return Plan{}, fmt.Errorf("planning horizon cannot be negative")
	}
	if limit <= 0 {
		limit = 100
	}
	if limit > 1000 {
		limit = 1000
	}
	attempts, err := m.store.PendingAttempts()
	if err != nil {
		return Plan{}, err
	}
	now := m.clk.Now()
	cutoff := now.Add(horizon)
	items := make([]PlanItem, 0, minInt(len(attempts), limit))
	result := Plan{GeneratedAt: now, Window: horizon.String()}
	for _, candidate := range attempts {
		if err := ctx.Err(); err != nil {
			return Plan{}, err
		}
		if len(items) >= limit {
			break
		}
		sub, err := m.store.GetSubscription(candidate.SubscriptionID)
		if err != nil {
			return Plan{}, err
		}
		policy := model.RetryPolicy{}
		if sub != nil {
			policy = sub.RetryPolicy
		}
		policy = policy.Normalized()
		ready := AttemptReady(candidate, now)
		within := !candidate.NextAttemptAt.After(cutoff)
		if horizon == 0 {
			within = ready
		}
		retryable := candidate.Retryable(policy)
		item := PlanItem{
			AttemptID: candidate.ID, SubscriptionID: candidate.SubscriptionID,
			EventType: candidate.EventType, Status: string(candidate.Status),
			AttemptCount: candidate.AttemptCount, MaxAttempts: policy.MaxAttempts,
			DueAt: candidate.NextAttemptAt, Ready: ready, WithinWindow: within,
			Retryable: retryable,
		}
		switch {
		case candidate.Status == model.StatusInFlight:
			item.Reason = "in-flight attempt waits for recovery or completion"
			result.Deferred++
		case !within:
			item.Reason = "scheduled after planning window"
			result.Deferred++
		case !retryable:
			item.Reason = "retry policy exhausted"
			result.RetryExhausted++
		case !ready:
			item.Reason = "backoff has not elapsed"
			result.Deferred++
		default:
			item.Reason = "ready for dispatch"
			result.Ready++
		}
		items = append(items, item)
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].DueAt.Equal(items[j].DueAt) {
			return items[i].AttemptID < items[j].AttemptID
		}
		return items[i].DueAt.Before(items[j].DueAt)
	})
	result.Total = len(items)
	result.Items = items
	return result, nil
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
