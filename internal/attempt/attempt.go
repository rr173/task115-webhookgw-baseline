// Package attempt provides read/aggregate/redelivery operations over delivery
// attempts and the dead-letter queue, built on top of the store.
package attempt

import (
	"time"

	"webhookgw/internal/model"
	"webhookgw/internal/store"

	"github.com/google/uuid"
)

// Service operates on attempts and dead letters.
type Service struct {
	store *store.Store
}

// New builds an attempt Service.
func New(s *store.Store) *Service { return &Service{store: s} }

// List returns a page of attempts plus the total matching count.
func (s *Service) List(f model.AttemptFilter) ([]*model.Attempt, int, error) {
	return s.store.ListAttempts(f)
}

// Get returns a single attempt.
func (s *Service) Get(id string) (*model.Attempt, error) {
	return s.store.GetAttempt(id)
}

// Stats aggregates attempt counts by status for a subscription.
func (s *Service) Stats(subID string) (map[string]int, int, error) {
	items, total, err := s.store.ListAttempts(model.AttemptFilter{SubscriptionID: subID, PageSize: 1000})
	if err != nil {
		return nil, 0, err
	}
	by := map[string]int{
		string(model.StatusPending):    0,
		string(model.StatusInFlight):   0,
		string(model.StatusDelivered):  0,
		string(model.StatusFailed):     0,
		string(model.StatusDeadLetter): 0,
	}
	for _, a := range items {
		by[string(a.Status)]++
	}
	return by, total, nil
}

// Retry requeues a single attempt for immediate delivery.
func (s *Service) Retry(id string) (*model.Attempt, error) {
	a, err := s.store.GetAttempt(id)
	if err != nil {
		return nil, err
	}
	if a == nil {
		return nil, nil
	}
	a.Status = model.StatusPending
	a.NextAttemptAt = time.Now()
	a.UpdatedAt = time.Now()
	if err := s.store.UpdateAttempt(a); err != nil {
		return nil, err
	}
	return a, nil
}

// DeadLetters lists the dead-letter queue.
func (s *Service) DeadLetters() ([]*model.DeadLetter, error) {
	return s.store.ListDeadLetters()
}

// ReplayDeadLetter requeues the attempt behind a dead-letter entry and removes
// it from the queue. It preserves the prior attempt history so subsequent
// deliveries build on the cumulative count rather than restarting from zero.
func (s *Service) ReplayDeadLetter(attemptID string) error {
	a, err := s.store.GetAttempt(attemptID)
	if err != nil {
		return err
	}
	if a == nil {
		return nil
	}
	a.Status = model.StatusPending
	a.NextAttemptAt = time.Now()
	a.UpdatedAt = time.Now()
	if err := s.store.UpdateAttempt(a); err != nil {
		return err
	}
	return s.store.DeleteDeadLetter(attemptID)
}

// DeleteDeadLetter drops a dead-letter entry without requeuing.
func (s *Service) DeleteDeadLetter(attemptID string) error {
	return s.store.DeleteDeadLetter(attemptID)
}

// NewEventID is a small helper used by the API when generating event ids.
func NewEventID() string { return uuid.NewString() }
