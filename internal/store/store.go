// Package store persists subscriptions, events, delivery attempts and dead
// letters in SQLite. It owns the only database handle and serializes writes so
// the embedded engine is never asked to do conflicting concurrent transactions.
package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"webhookgw/internal/model"

	_ "modernc.org/sqlite"
)

// Store is the persistence layer.
type Store struct {
	db *sql.DB
	mu sync.Mutex
}

// Open opens (creating if needed) the SQLite database at path and applies the
// schema. The caller must Close the store.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("busy_timeout: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the database handle.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) exec(query string, args ...interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.Exec(query, args...)
	return err
}

func (s *Store) query(query string, args ...interface{}) (*sql.Rows, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Query(query, args...)
}

// ---- subscriptions ----

func (s *Store) CreateSubscription(sub *model.Subscription) error {
	events, err := json.Marshal(sub.Events)
	if err != nil {
		return err
	}
	return s.exec(
		`INSERT INTO subscriptions
         (id,name,endpoint,events,signing_secret,rate_limit,max_attempts,base_delay_ms,max_delay_ms,created_at,updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		sub.ID, sub.Name, sub.Endpoint, string(events), sub.SigningSecret,
		sub.RateLimit, sub.RetryPolicy.MaxAttempts, sub.RetryPolicy.BaseDelay.Milliseconds(),
		sub.RetryPolicy.MaxDelay.Milliseconds(), sub.CreatedAt.UnixMilli(), sub.UpdatedAt.UnixMilli())
}

func scanSubscription(rows *sql.Rows) (*model.Subscription, error) {
	var (
		id, name, endpoint, events, secret                                string
		rateLimit, maxAttempts, baseDelay, maxDelay, createdAt, updatedAt int64
	)
	if err := rows.Scan(&id, &name, &endpoint, &events, &secret, &rateLimit, &maxAttempts, &baseDelay, &maxDelay, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	var ev []string
	_ = json.Unmarshal([]byte(events), &ev)
	return &model.Subscription{
		ID: id, Name: name, Endpoint: endpoint, Events: ev, SigningSecret: secret,
		RateLimit: int(rateLimit),
		RetryPolicy: model.RetryPolicy{
			MaxAttempts: int(maxAttempts),
			BaseDelay:   time.Duration(baseDelay) * time.Millisecond,
			MaxDelay:    time.Duration(maxDelay) * time.Millisecond,
		},
		CreatedAt: time.UnixMilli(createdAt), UpdatedAt: time.UnixMilli(updatedAt),
	}, nil
}

func (s *Store) GetSubscription(id string) (*model.Subscription, error) {
	rows, err := s.query(`SELECT id,name,endpoint,events,signing_secret,rate_limit,max_attempts,base_delay_ms,max_delay_ms,created_at,updated_at FROM subscriptions WHERE id=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return scanSubscription(rows)
}

func (s *Store) ListSubscriptions() ([]*model.Subscription, error) {
	rows, err := s.query(`SELECT id,name,endpoint,events,signing_secret,rate_limit,max_attempts,base_delay_ms,max_delay_ms,created_at,updated_at FROM subscriptions ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Subscription
	for rows.Next() {
		sub, err := scanSubscription(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, sub)
	}
	return out, rows.Err()
}

func (s *Store) UpdateSubscription(sub *model.Subscription) error {
	events, err := json.Marshal(sub.Events)
	if err != nil {
		return err
	}
	return s.exec(
		`UPDATE subscriptions SET name=?,endpoint=?,events=?,rate_limit=?,max_attempts=?,base_delay_ms=?,max_delay_ms=?,updated_at=? WHERE id=?`,
		sub.Name, sub.Endpoint, string(events), sub.RateLimit, sub.RetryPolicy.MaxAttempts,
		sub.RetryPolicy.BaseDelay.Milliseconds(), sub.RetryPolicy.MaxDelay.Milliseconds(),
		sub.UpdatedAt.UnixMilli(), sub.ID)
}

func (s *Store) DeleteSubscription(id string) error {
	return s.exec(`DELETE FROM subscriptions WHERE id=?`, id)
}

func (s *Store) RotateSecret(id, secret string, updatedAt time.Time) error {
	return s.exec(`UPDATE subscriptions SET signing_secret=?, updated_at=? WHERE id=?`, secret, updatedAt.UnixMilli(), id)
}

// ---- events ----

func (s *Store) SaveEvent(e *model.Event) error {
	return s.exec(`INSERT INTO events (id,type,payload,created_at) VALUES (?,?,?,?)`, e.ID, e.Type, e.Payload, e.CreatedAt.UnixMilli())
}

func (s *Store) ListEvents(limit int) ([]*model.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.query(`SELECT id,type,payload,created_at FROM events ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Event
	for rows.Next() {
		var id, typ, payload string
		var createdAt int64
		if err := rows.Scan(&id, &typ, &payload, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, &model.Event{ID: id, Type: typ, Payload: payload, CreatedAt: time.UnixMilli(createdAt)})
	}
	return out, rows.Err()
}

// ---- attempts ----

func (s *Store) SaveAttempt(a *model.Attempt) error {
	return s.exec(
		`INSERT INTO attempts (id,subscription_id,event_id,event_type,payload,status,attempt_count,last_error,next_attempt_at,created_at,updated_at)
         VALUES (?,?,?,?,?,?,?,?,?,?,?)`,
		a.ID, a.SubscriptionID, a.EventID, a.EventType, a.Payload, string(a.Status),
		a.AttemptCount, a.LastError, a.NextAttemptAt.UnixMilli(), a.CreatedAt.UnixMilli(), a.UpdatedAt.UnixMilli())
}

func scanAttempt(rows *sql.Rows) (*model.Attempt, error) {
	var (
		id, subID, eventID, eventType, payload, status, lastError string
		attemptCount, nextAt, createdAt, updatedAt                int64
	)
	if err := rows.Scan(&id, &subID, &eventID, &eventType, &payload, &status, &attemptCount, &lastError, &nextAt, &createdAt, &updatedAt); err != nil {
		return nil, err
	}
	return &model.Attempt{
		ID: id, SubscriptionID: subID, EventID: eventID, EventType: eventType, Payload: payload,
		Status:        model.AttemptStatus(status),
		AttemptCount:  int(attemptCount),
		LastError:     lastError,
		NextAttemptAt: time.UnixMilli(nextAt),
		CreatedAt:     time.UnixMilli(createdAt),
		UpdatedAt:     time.UnixMilli(updatedAt),
	}, nil
}

func (s *Store) GetAttempt(id string) (*model.Attempt, error) {
	rows, err := s.query(`SELECT id,subscription_id,event_id,event_type,payload,status,attempt_count,last_error,next_attempt_at,created_at,updated_at FROM attempts WHERE id=?`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	return scanAttempt(rows)
}

func (s *Store) UpdateAttempt(a *model.Attempt) error {
	return s.exec(
		`UPDATE attempts SET status=?, attempt_count=?, last_error=?, next_attempt_at=?, updated_at=? WHERE id=?`,
		string(a.Status), a.AttemptCount, a.LastError, a.NextAttemptAt.UnixMilli(), a.UpdatedAt.UnixMilli(), a.ID)
}

// ListAttempts returns a page of attempts matching the filter plus the total
// count across all matching rows (for pagination metadata).
func (s *Store) ListAttempts(f model.AttemptFilter) ([]*model.Attempt, int, error) {
	var conds []string
	var args []interface{}
	if f.SubscriptionID != "" {
		conds = append(conds, "subscription_id=?")
		args = append(args, f.SubscriptionID)
	}
	if f.EventID != "" {
		conds = append(conds, "event_id=?")
		args = append(args, f.EventID)
	}
	if f.Status != "" {
		conds = append(conds, "status=?")
		args = append(args, string(f.Status))
	}
	where := ""
	if len(conds) > 0 {
		where = " WHERE " + strings.Join(conds, " AND ")
	}
	totalRows, err := s.query(`SELECT COUNT(*) FROM attempts`+where, args...)
	if err != nil {
		return nil, 0, err
	}
	var total int
	if totalRows.Next() {
		if err := totalRows.Scan(&total); err != nil {
			totalRows.Close()
			return nil, 0, err
		}
	}
	totalRows.Close()

	offset, limit := f.PageBounds()
	// Newest first: page 1 returns the most recent deliveries, and the tie-
	// breaker on id keeps the order (and thus pagination) stable across rows
	// that share a created_at millisecond.
	q := `SELECT id,subscription_id,event_id,event_type,payload,status,attempt_count,last_error,next_attempt_at,created_at,updated_at FROM attempts` + where + ` ORDER BY created_at DESC, id DESC LIMIT ? OFFSET ?`
	rows, err := s.query(q, append(args, limit, offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var out []*model.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, 0, err
		}
		out = append(out, a)
	}
	return out, total, rows.Err()
}

func (s *Store) PendingAttempts() ([]*model.Attempt, error) {
	rows, err := s.query(`SELECT id,subscription_id,event_id,event_type,payload,status,attempt_count,last_error,next_attempt_at,created_at,updated_at FROM attempts WHERE status IN (?,?) ORDER BY next_attempt_at ASC`, string(model.StatusPending), string(model.StatusInFlight))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.Attempt
	for rows.Next() {
		a, err := scanAttempt(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ---- dead letters ----

func (s *Store) SaveDeadLetter(d *model.DeadLetter) error {
	return s.exec(
		`INSERT OR REPLACE INTO dead_letters (attempt_id,subscription_id,event_type,payload,reason,attempt_count,created_at) VALUES (?,?,?,?,?,?,?)`,
		d.AttemptID, d.SubscriptionID, d.EventType, d.Payload, d.Reason, d.AttemptCount, d.CreatedAt.UnixMilli())
}

func (s *Store) ListDeadLetters() ([]*model.DeadLetter, error) {
	rows, err := s.query(`SELECT attempt_id,subscription_id,event_type,payload,reason,attempt_count,created_at FROM dead_letters ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*model.DeadLetter
	for rows.Next() {
		var attemptID, subID, eventType, payload, reason string
		var attemptCount, createdAt int64
		if err := rows.Scan(&attemptID, &subID, &eventType, &payload, &reason, &attemptCount, &createdAt); err != nil {
			return nil, err
		}
		out = append(out, &model.DeadLetter{
			AttemptID: attemptID, SubscriptionID: subID, EventType: eventType,
			Payload: payload, Reason: reason, AttemptCount: int(attemptCount),
			CreatedAt: time.UnixMilli(createdAt),
		})
	}
	return out, rows.Err()
}

func (s *Store) DeleteDeadLetter(attemptID string) error {
	return s.exec(`DELETE FROM dead_letters WHERE attempt_id=?`, attemptID)
}
