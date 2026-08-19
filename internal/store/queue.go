package store

import (
	"database/sql"
	"time"

	"webhookgw/internal/model"
)

// QueueSummary describes durable attempt state independently of process-local
// metrics. This distinction matters after a restart, when SQLite still knows
// about delivered and recovered attempts but the in-memory counters are new.
type QueueSummary struct {
	Total         int        `json:"total"`
	Pending       int        `json:"pending"`
	InFlight      int        `json:"in_flight"`
	Delivered     int        `json:"delivered"`
	Failed        int        `json:"failed"`
	DeadLetters   int        `json:"dead_letters"`
	OldestDueAt   *time.Time `json:"oldest_due_at,omitempty"`
	LatestUpdated *time.Time `json:"latest_updated_at,omitempty"`
}

// IsStalled reports whether work has been due longer than threshold without
// the queue being empty. It intentionally uses durable state so a restart
// cannot hide a stalled delivery from an operator.
func (q QueueSummary) IsStalled(now time.Time, threshold time.Duration) bool {
	if q.Pending == 0 || q.OldestDueAt == nil || threshold < 0 {
		return false
	}
	return !q.OldestDueAt.Add(threshold).After(now)
}

// DueWithin reports whether pending work is due by the supplied cutoff.
func (q QueueSummary) DueWithin(cutoff time.Time) bool {
	return q.Pending > 0 && q.OldestDueAt != nil && !q.OldestDueAt.After(cutoff)
}

// PendingRatio gives dashboards a stable measure of how much durable work is
// waiting compared with all recorded attempts.
func (q QueueSummary) PendingRatio() float64 {
	if q.Total == 0 {
		return 0
	}
	return float64(q.Pending) / float64(q.Total)
}

// StallAge returns the age of the oldest due attempt, or zero when none is
// waiting. Callers can use it for alerts without interpreting nullable SQL
// timestamps themselves.
func (q QueueSummary) StallAge(now time.Time) time.Duration {
	if q.OldestDueAt == nil || now.Before(*q.OldestDueAt) {
		return 0
	}
	return now.Sub(*q.OldestDueAt)
}

// RecoveryRequired indicates that the last process stopped during delivery.
func (q QueueSummary) RecoveryRequired() bool {
	return q.InFlight > 0
}

// Active reports whether any attempt is still part of the delivery lifecycle.
func (q QueueSummary) Active() bool {
	return q.Pending > 0 || q.InFlight > 0
}

// IsEmpty distinguishes a drained queue from a queue that only contains
// historical terminal attempts.
func (q QueueSummary) IsEmpty() bool {
	return !q.Active()
}

// HasHistory reports whether the durable store contains any attempt record.
func (q QueueSummary) HasHistory() bool {
	return q.Total > 0
}

// QueueSummary returns counts for every persisted attempt status and the two
// timestamps operators need when deciding whether a queue is stalled.
func (s *Store) QueueSummary() (QueueSummary, error) {
	rows, err := s.query(`SELECT status, COUNT(*) FROM attempts GROUP BY status`)
	if err != nil {
		return QueueSummary{}, err
	}
	var out QueueSummary
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			rows.Close()
			return QueueSummary{}, err
		}
		out.Total += count
		switch model.AttemptStatus(status) {
		case model.StatusPending:
			out.Pending = count
		case model.StatusInFlight:
			out.InFlight = count
		case model.StatusDelivered:
			out.Delivered = count
		case model.StatusFailed:
			out.Failed = count
		case model.StatusDeadLetter:
			out.DeadLetters = count
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return QueueSummary{}, err
	}
	rows.Close()
	if err := s.readQueueTimes(&out); err != nil {
		return QueueSummary{}, err
	}
	return out, nil
}

func (s *Store) readQueueTimes(out *QueueSummary) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest, latest sql.NullInt64
	row := s.db.QueryRow(`
		SELECT MIN(CASE WHEN status IN (?,?) THEN next_attempt_at END),
		       MAX(updated_at)
		FROM attempts`, string(model.StatusPending), string(model.StatusInFlight))
	if err := row.Scan(&oldest, &latest); err != nil {
		return err
	}
	if oldest.Valid {
		value := time.UnixMilli(oldest.Int64)
		out.OldestDueAt = &value
	}
	if latest.Valid {
		value := time.UnixMilli(latest.Int64)
		out.LatestUpdated = &value
	}
	return nil
}
