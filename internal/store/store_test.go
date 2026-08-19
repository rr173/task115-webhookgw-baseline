package store

import (
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
