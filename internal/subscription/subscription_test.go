package subscription

import (
	"testing"

	"webhookgw/internal/clock"
	"webhookgw/internal/model"
	"webhookgw/internal/store"
)

func mustOpen(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(t.TempDir() + "/t.db")
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestCreateAndMatches(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	s := New(st, clock.RealClock{})
	sub, err := s.Create("n", "https://example.com/h", nil, 0, model.RetryPolicy{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if sub.SigningSecret == "" {
		t.Fatal("signing secret was not generated")
	}
	if !s.Matches(sub, "anything") {
		t.Fatal("empty event list should match all event types")
	}
	sub2, err := s.Create("n2", "https://example.com/h2", []string{"a", "b"}, 0, model.RetryPolicy{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	if !s.Matches(sub2, "a") {
		t.Fatal("should match subscribed event a")
	}
	if s.Matches(sub2, "c") {
		t.Fatal("should not match unsubscribed event c")
	}
}

func TestInvalidEndpoint(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	s := New(st, clock.RealClock{})
	if _, err := s.Create("n", "not-a-url", nil, 0, model.RetryPolicy{MaxAttempts: 3}); err == nil {
		t.Fatal("expected error for invalid endpoint URL")
	}
}

func TestRotateSecret(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	s := New(st, clock.RealClock{})
	sub, _ := s.Create("n", "https://e/h", nil, 0, model.RetryPolicy{MaxAttempts: 3})
	old := sub.SigningSecret
	sub2, err := s.RotateSecret(sub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if sub2.SigningSecret == old {
		t.Fatal("secret was not rotated")
	}
}
