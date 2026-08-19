// Package subscription manages webhook subscription lifecycle: creation,
// validation, rotation of signing secrets and event-type matching.
package subscription

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/url"
	"strings"

	"webhookgw/internal/clock"
	"webhookgw/internal/model"
	"webhookgw/internal/store"

	"github.com/google/uuid"
)

// Service operates on subscriptions backed by a Store.
type Service struct {
	store *store.Store
	clk   clock.Clock
}

// New builds a subscription Service.
func New(s *store.Store, clk clock.Clock) *Service {
	return &Service{store: s, clk: clk}
}

// Create registers a new subscription. Events may be empty to mean "all types".
func (s *Service) Create(name, endpoint string, events []string, rateLimit int, policy model.RetryPolicy) (*model.Subscription, error) {
	if strings.TrimSpace(name) == "" {
		return nil, fmt.Errorf("subscription name is required")
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("invalid endpoint URL: %w", err)
	}
	policy = policy.Normalized()
	now := s.clk.Now()
	sub := &model.Subscription{
		ID:            uuid.NewString(),
		Name:          name,
		Endpoint:      endpoint,
		Events:        normalizeEvents(events),
		SigningSecret: randomSecret(),
		RateLimit:     rateLimit,
		RetryPolicy:   policy,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	if err := s.store.CreateSubscription(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

// Get returns a subscription by ID, or nil if absent.
func (s *Service) Get(id string) (*model.Subscription, error) {
	return s.store.GetSubscription(id)
}

// List returns all subscriptions.
func (s *Service) List() ([]*model.Subscription, error) {
	return s.store.ListSubscriptions()
}

// Update changes mutable fields of an existing subscription.
func (s *Service) Update(id, name, endpoint string, events []string, rateLimit int, policy model.RetryPolicy) (*model.Subscription, error) {
	sub, err := s.store.GetSubscription(id)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, fmt.Errorf("subscription %s not found", id)
	}
	if _, err := url.ParseRequestURI(endpoint); err != nil {
		return nil, fmt.Errorf("invalid endpoint URL: %w", err)
	}
	sub.Name = name
	sub.Endpoint = endpoint
	sub.Events = normalizeEvents(events)
	sub.RateLimit = rateLimit
	sub.RetryPolicy = policy.Normalized()
	sub.UpdatedAt = s.clk.Now()
	if err := sub.Validate(); err != nil {
		return nil, err
	}
	if err := s.store.UpdateSubscription(sub); err != nil {
		return nil, err
	}
	return sub, nil
}

// Delete removes a subscription.
func (s *Service) Delete(id string) error {
	return s.store.DeleteSubscription(id)
}

// RotateSecret issues a new signing secret.
func (s *Service) RotateSecret(id string) (*model.Subscription, error) {
	sub, err := s.store.GetSubscription(id)
	if err != nil {
		return nil, err
	}
	if sub == nil {
		return nil, fmt.Errorf("subscription %s not found", id)
	}
	oldSecret := sub.SigningSecret
	sub.SigningSecret = randomSecret()
	sub.UpdatedAt = s.clk.Now()
	if err := s.store.RotateSecret(id, oldSecret, sub.UpdatedAt); err != nil {
		return nil, err
	}
	return sub, nil
}

// Matches reports whether a subscription should receive an event of eventType.
// An empty Events list means the subscription receives all event types.
func (s *Service) Matches(sub *model.Subscription, eventType string) bool {
	if len(sub.Events) == 0 {
		return true
	}
	for _, e := range sub.Events {
		if e == eventType {
			return true
		}
	}
	return false
}

func normalizeEvents(events []string) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		e = strings.TrimSpace(e)
		if e != "" {
			out = append(out, e)
		}
	}
	return out
}

func randomSecret() string {
	b := make([]byte, 24)
	if _, err := rand.Read(b); err != nil {
		return uuid.NewString()
	}
	return "whsec_" + hex.EncodeToString(b)
}
