// Package model defines the domain types shared across the webhook delivery
// gateway: subscriptions, ingested events, delivery attempts and dead letters.
package model

import "time"

// Subscription registers an endpoint that should receive events of certain
// types, signed with HMAC, subject to a per-endpoint rate limit and retry
// policy.
type Subscription struct {
	ID            string
	Name          string
	Endpoint      string
	Events        []string
	SigningSecret string
	RateLimit     int
	RetryPolicy   RetryPolicy
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// RetryPolicy controls how failed deliveries are retried.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// Event is an ingested occurrence that may trigger one delivery attempt per
// matching subscription.
type Event struct {
	ID        string
	Type      string
	Payload   string
	CreatedAt time.Time
}

// AttemptStatus is the lifecycle state of a delivery attempt.
type AttemptStatus string

const (
	StatusPending    AttemptStatus = "pending"
	StatusInFlight   AttemptStatus = "in_flight"
	StatusDelivered  AttemptStatus = "delivered"
	StatusFailed     AttemptStatus = "failed"
	StatusDeadLetter AttemptStatus = "dead_letter"
)

// Attempt is a single delivery of an event to a subscription endpoint.
type Attempt struct {
	ID             string
	SubscriptionID string
	EventID        string
	EventType      string
	Payload        string
	Status         AttemptStatus
	AttemptCount   int
	LastError      string
	NextAttemptAt  time.Time
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// DeadLetter is a permanently failed attempt awaiting operator replay.
type DeadLetter struct {
	AttemptID      string
	SubscriptionID string
	EventType      string
	Payload        string
	Reason         string
	AttemptCount   int
	CreatedAt      time.Time
}

// AttemptFilter narrows a ListAttempts query.
type AttemptFilter struct {
	SubscriptionID string
	EventID        string
	Status         AttemptStatus
	Page           int
	PageSize       int
}

// PageBounds converts the 1-based Page/PageSize into SQL offset/limit, applying
// sane defaults and clamping. Page 1 maps to offset 0 so the first page starts
// at the beginning of the result set, not one page in.
func (f AttemptFilter) PageBounds() (offset, limit int) {
	if f.PageSize <= 0 {
		limit = 20
	} else {
		limit = f.PageSize
	}
	if f.Page <= 1 {
		return 0, limit
	}
	return (f.Page - 1) * limit, limit
}
