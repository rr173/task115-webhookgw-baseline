package model

import "fmt"

func (s Subscription) Validate() error {
	if s.ID == "" || s.Endpoint == "" {
		return fmt.Errorf("subscription id and endpoint required")
	}
	if s.RateLimit < 0 {
		return fmt.Errorf("rate limit must be non-negative")
	}
	seen := make(map[string]struct{}, len(s.Events))
	for _, event := range s.Events {
		if event == "" {
			return fmt.Errorf("subscription event type cannot be empty")
		}
		if _, ok := seen[event]; ok {
			return fmt.Errorf("subscription event type %q is duplicated", event)
		}
		seen[event] = struct{}{}
	}
	return nil
}
func (e Event) Validate() error {
	if e.ID == "" || e.Type == "" {
		return fmt.Errorf("event id and type required")
	}
	if len(e.Payload) > 1<<20 {
		return fmt.Errorf("event payload exceeds 1 MiB")
	}
	return nil
}
func (a Attempt) Retryable(policy RetryPolicy) bool {
	policy = policy.Normalized()
	if a.Status != StatusFailed && a.Status != StatusPending {
		return false
	}
	return a.AttemptCount+1 < policy.MaxAttempts
}
