// Package metrics tracks delivery counters. The dispatcher updates counters
// from many worker goroutines while /metrics reads them, so every field is
// guarded by a single mutex and Snapshot returns an immutable copy.
package metrics

import "sync"

// Counters holds aggregate delivery statistics.
type Counters struct {
	Delivered    int64
	Failed       int64
	DeadLettered int64
	InFlight     int64
}

// SubCounters holds per-subscription statistics.
type SubCounters struct {
	SubscriptionID string
	Counters
}

// Metrics is a concurrency-safe counter store.
type Metrics struct {
	mu     sync.Mutex
	total  Counters
	perSub map[string]*Counters
}

// New returns an empty Metrics.
func New() *Metrics {
	return &Metrics{perSub: make(map[string]*Counters)}
}

// RecordDelivered increments the delivered counters.
func (m *Metrics) RecordDelivered(subID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total.Delivered++
	m.sub(subID).Delivered++
}

// RecordFailed increments the failed counters.
func (m *Metrics) RecordFailed(subID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total.Failed++
	m.sub(subID).Failed++
}

// RecordDeadLetter increments the dead-letter counters.
func (m *Metrics) RecordDeadLetter(subID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total.Failed++
	m.sub(subID).Failed++
}

// IncInFlight / DecInFlight track in-flight deliveries.
func (m *Metrics) IncInFlight() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.total.InFlight++
}

// DecInFlight decrements the in-flight counter.
func (m *Metrics) DecInFlight() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.total.InFlight > 0 {
		m.total.InFlight--
	}
}

func (m *Metrics) sub(subID string) *Counters {
	c, ok := m.perSub[subID]
	if !ok {
		c = &Counters{}
		m.perSub[subID] = c
	}
	return c
}

// Snapshot returns a copy of the current totals and per-subscription counters.
func (m *Metrics) Snapshot() (Counters, []SubCounters) {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := m.total
	subs := make([]SubCounters, 0, len(m.perSub))
	for id, c := range m.perSub {
		subs = append(subs, SubCounters{SubscriptionID: id, Counters: *c})
	}
	return total, subs
}
