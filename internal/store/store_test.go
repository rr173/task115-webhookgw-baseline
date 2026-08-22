package store

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"webhookgw/internal/model"
)

func mustOpen(t *testing.T) *Store {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestSubscriptionCRUD(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	now := time.Now()
	sub := &model.Subscription{
		ID: "s1", Name: "n", Endpoint: "https://e/x", Events: []string{"a"},
		SigningSecret: "sec", RateLimit: 0,
		RetryPolicy: model.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Second},
		CreatedAt:   now, UpdatedAt: now,
	}
	if err := st.CreateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetSubscription("s1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Name != "n" || len(got.Events) != 1 {
		t.Fatalf("get: %+v", got)
	}
	sub.Name = "n2"
	if err := st.UpdateSubscription(sub); err != nil {
		t.Fatal(err)
	}
	got, _ = st.GetSubscription("s1")
	if got.Name != "n2" {
		t.Fatalf("update: %+v", got)
	}
	list, _ := st.ListSubscriptions()
	if len(list) != 1 {
		t.Fatalf("list len %d", len(list))
	}
	if err := st.DeleteSubscription("s1"); err != nil {
		t.Fatal(err)
	}
	got, _ = st.GetSubscription("s1")
	if got != nil {
		t.Fatal("delete did not remove subscription")
	}
}

func TestAttemptListAndDeadLetter(t *testing.T) {
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
	items, total, err := st.ListAttempts(model.AttemptFilter{SubscriptionID: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("list: total=%d items=%d", total, len(items))
	}
	dl := &model.DeadLetter{
		AttemptID: "a1", SubscriptionID: "s1", EventType: "t", Payload: "{}",
		Reason: "r", AttemptCount: 1, CreatedAt: now,
	}
	if err := st.SaveDeadLetter(dl); err != nil {
		t.Fatal(err)
	}
	dls, err := st.ListDeadLetters()
	if err != nil {
		t.Fatal(err)
	}
	if len(dls) != 1 {
		t.Fatalf("dead letters: %d", len(dls))
	}
	if err := st.DeleteDeadLetter("a1"); err != nil {
		t.Fatal(err)
	}
	dls, _ = st.ListDeadLetters()
	if len(dls) != 0 {
		t.Fatalf("after delete: %d", len(dls))
	}
}

func TestEventSaveList(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	e := &model.Event{ID: "e1", Type: "t", Payload: "{}", CreatedAt: time.Now()}
	if err := st.SaveEvent(e); err != nil {
		t.Fatal(err)
	}
	evs, err := st.ListEvents(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(evs) != 1 {
		t.Fatalf("events: %d", len(evs))
	}
}

// TestAttemptListPaginationNewestFirst pins the pagination contract for the
// delivery log: page 1 must return the most recent attempts first, and pages
// must be contiguous — walking page 1..N yields every row exactly once in
// strict newest-to-oldest order. A regression here surfaces as the first page
// starting mid-history and the newest deliveries never appearing until the
// last page.
func TestAttemptListPaginationNewestFirst(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	base := time.Now().Truncate(time.Millisecond)
	// Insert 5 attempts with strictly increasing created_at / id so the
	// newest is unambiguously the last-inserted one.
	const n = 5
	for i := 0; i < n; i++ {
		a := &model.Attempt{
			ID:             fmt.Sprintf("a%d", i),
			SubscriptionID: "s1",
			EventID:        "e",
			EventType:      "t",
			Payload:        "{}",
			Status:         model.StatusDelivered,
			AttemptCount:   1,
			NextAttemptAt:  base,
			CreatedAt:      base.Add(time.Duration(i) * time.Millisecond),
			UpdatedAt:      base,
		}
		if err := st.SaveAttempt(a); err != nil {
			t.Fatal(err)
		}
	}

	// Page 1 (size 2) must lead with the newest attempt, not the oldest.
	page1, total, err := st.ListAttempts(model.AttemptFilter{SubscriptionID: "s1", Page: 1, PageSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	if total != n {
		t.Fatalf("total: got %d want %d", total, n)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len: %d", len(page1))
	}
	if page1[0].ID != "a4" || page1[1].ID != "a3" {
		t.Fatalf("page1 order: got %s,%s want a4,a3", page1[0].ID, page1[1].ID)
	}

	// Walk all pages; the concatenation must be exactly the full set with no
	// gaps, no duplicates, and strictly descending created_at — i.e. the
	// newest-first ordering is preserved across the page boundary.
	var got []string
	var prev time.Time
	for page := 1; ; page++ {
		items, _, err := st.ListAttempts(model.AttemptFilter{SubscriptionID: "s1", Page: page, PageSize: 2})
		if err != nil {
			t.Fatal(err)
		}
		if len(items) == 0 {
			break
		}
		for _, it := range items {
			got = append(got, it.ID)
			if !prev.IsZero() && !it.CreatedAt.Before(prev) {
				t.Fatalf("page %d: %s created_at not descending", page, it.ID)
			}
			prev = it.CreatedAt
		}
		if len(got) >= n {
			break
		}
	}
	want := []string{"a4", "a3", "a2", "a1", "a0"}
	if len(got) != n {
		t.Fatalf("walked %d items, want %d", len(got), n)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("walk order at %d: got %s want %s", i, got[i], want[i])
		}
	}
}
