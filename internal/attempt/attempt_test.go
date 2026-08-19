package attempt

import (
	"path/filepath"
	"testing"
	"time"

	"webhookgw/internal/model"
	"webhookgw/internal/store"
)

func mustOpen(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestStatsAndRetry(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	now := time.Now()
	a := &model.Attempt{
		ID: "a1", SubscriptionID: "s1", EventID: "e1", EventType: "t", Payload: "{}",
		Status: model.StatusDelivered, AttemptCount: 1, NextAttemptAt: now,
		CreatedAt: now, UpdatedAt: now,
	}
	if err := st.SaveAttempt(a); err != nil {
		t.Fatal(err)
	}
	s := New(st)
	by, total, err := s.Stats("s1")
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("total = %d", total)
	}
	if by[string(model.StatusDelivered)] != 1 {
		t.Fatalf("by status = %v", by)
	}
	a.Status = model.StatusPending
	if err := st.UpdateAttempt(a); err != nil {
		t.Fatal(err)
	}
	got, err := s.Retry("a1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Status != model.StatusPending {
		t.Fatalf("retry = %+v", got)
	}
}
