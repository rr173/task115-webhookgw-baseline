package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"webhookgw/internal/attempt"
	"webhookgw/internal/clock"
	"webhookgw/internal/delivery"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/store"
	"webhookgw/internal/subscription"

	"github.com/google/uuid"
)

// runSmokeTest exercises the core delivery contract against temporary SQLite
// files and an in-process HTTP endpoint. It validates successful delivery,
// restart recovery (persistence), the dead-letter path on persistent 5xx, and
// Recover() resetting in-flight attempts. It uses a FakeClock so no real sleep
// is needed.
func runSmokeTest() error {
	dir, err := os.MkdirTemp("", "webhookgw-smoke-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	if err := smokeHappyPathAndRestart(filepath.Join(dir, "happy.db")); err != nil {
		return err
	}
	if err := smokeDeadLetter(filepath.Join(dir, "dl.db")); err != nil {
		return err
	}
	if err := smokeRecoverInFlight(filepath.Join(dir, "recover.db")); err != nil {
		return err
	}
	return nil
}

func smokeHappyPathAndRestart(dbPath string) error {
	received := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	subs := subscription.New(st, clock.RealClock{})
	d := delivery.New(st, subs, metrics.New(), clock.RealClock{}, srv.Client())

	sub, err := subs.Create("smoke", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Second})
	if err != nil {
		st.Close()
		return err
	}
	ev := &model.Event{ID: uuid.NewString(), Type: "order.created", Payload: `{"ok":true}`}
	if err := st.SaveEvent(ev); err != nil {
		st.Close()
		return err
	}
	if _, err := d.Enqueue(ev); err != nil {
		st.Close()
		return err
	}
	if err := d.Dispatch(context.Background()); err != nil {
		st.Close()
		return err
	}
	select {
	case body := <-received:
		if body != ev.Payload {
			st.Close()
			return fmt.Errorf("happy: payload mismatch: got %q", body)
		}
	case <-time.After(3 * time.Second):
		st.Close()
		return fmt.Errorf("happy: no delivery received")
	}
	items, _, err := attempt.New(st).List(model.AttemptFilter{SubscriptionID: sub.ID})
	if err != nil {
		st.Close()
		return err
	}
	if len(items) == 0 || items[0].Status != model.StatusDelivered {
		st.Close()
		return fmt.Errorf("happy: attempt not delivered: %v", items)
	}

	// Restart: reopen and confirm the delivered state persisted.
	st.Close()
	st2, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st2.Close()
	items2, _, err := attempt.New(st2).List(model.AttemptFilter{SubscriptionID: sub.ID})
	if err != nil {
		return err
	}
	if len(items2) == 0 || items2[0].Status != model.StatusDelivered {
		return fmt.Errorf("restart: delivered state not persisted: %v", items2)
	}
	return nil
}

func smokeDeadLetter(dbPath string) error {
	clk := clock.NewFakeClock(time.Unix(1_000_000, 0))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	subs := subscription.New(st, clk)
	d := delivery.New(st, subs, metrics.New(), clk, srv.Client())

	sub, err := subs.Create("dl", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Second})
	if err != nil {
		return err
	}
	ev := &model.Event{ID: uuid.NewString(), Type: "order.failed", Payload: `{"x":1}`}
	if err := st.SaveEvent(ev); err != nil {
		return err
	}
	if _, err := d.Enqueue(ev); err != nil {
		return err
	}
	if err := d.Dispatch(context.Background()); err != nil {
		return err
	}
	clk.Advance(2 * time.Second)
	if err := d.Dispatch(context.Background()); err != nil {
		return err
	}
	items, _, err := attempt.New(st).List(model.AttemptFilter{SubscriptionID: sub.ID})
	if err != nil {
		return err
	}
	if len(items) == 0 || items[0].Status != model.StatusDeadLetter {
		return fmt.Errorf("deadletter: expected dead_letter, got %v", items)
	}
	dlq, err := attempt.New(st).DeadLetters()
	if err != nil {
		return err
	}
	if len(dlq) != 1 {
		return fmt.Errorf("deadletter: expected 1 dead letter, got %d", len(dlq))
	}
	return nil
}

func smokeRecoverInFlight(dbPath string) error {
	st, err := store.Open(dbPath)
	if err != nil {
		return err
	}
	defer st.Close()
	subs := subscription.New(st, clock.RealClock{})
	d := delivery.New(st, subs, metrics.New(), clock.RealClock{}, nil)

	sub, err := subs.Create("recover", "http://127.0.0.1:9/nope", nil, 0, model.RetryPolicy{MaxAttempts: 5})
	if err != nil {
		return err
	}
	now := time.Now()
	a := &model.Attempt{
		ID:             uuid.NewString(),
		SubscriptionID: sub.ID,
		EventID:        uuid.NewString(),
		EventType:      "x",
		Payload:        "{}",
		Status:         model.StatusInFlight,
		AttemptCount:   1,
		NextAttemptAt:  now.Add(time.Hour),
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := st.SaveAttempt(a); err != nil {
		return err
	}
	n, err := d.Recover()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("recover: expected 1 reset, got %d", n)
	}
	a2, err := st.GetAttempt(a.ID)
	if err != nil {
		return err
	}
	if a2.Status != model.StatusPending {
		return fmt.Errorf("recover: status not reset: %s", a2.Status)
	}
	if a2.NextAttemptAt.After(time.Now()) {
		return fmt.Errorf("recover: nextAttemptAt not reset to now")
	}
	return nil
}
