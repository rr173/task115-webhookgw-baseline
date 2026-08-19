package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"webhookgw/internal/attempt"
	"webhookgw/internal/clock"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/store"
	"webhookgw/internal/subscription"

	"github.com/google/uuid"
)

func openTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestEnqueueDelivers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clock.RealClock{})
	sub, err := subs.Create("s", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m := New(st, subs, metrics.New(), clock.RealClock{}, srv.Client())

	ev := &model.Event{ID: uuid.NewString(), Type: "x", Payload: "{}"}
	if err := st.SaveEvent(ev); err != nil {
		t.Fatal(err)
	}
	n, err := m.Enqueue(ev)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("want 1 attempt, got %d", n)
	}
	if err := m.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	items, _, err := attempt.New(st).List(model.AttemptFilter{SubscriptionID: sub.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) == 0 || items[0].Status != model.StatusDelivered {
		t.Fatalf("want delivered, got %+v", items)
	}
}

func TestDispatchDeadLettersOnPersistent5xx(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(1, 0))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	sub, err := subs.Create("dl", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	m := New(st, subs, metrics.New(), clk, srv.Client())

	ev := &model.Event{ID: uuid.NewString(), Type: "x", Payload: "{}"}
	if err := st.SaveEvent(ev); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Enqueue(ev); err != nil {
		t.Fatal(err)
	}
	if err := m.Dispatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	items, _, err := attempt.New(st).List(model.AttemptFilter{SubscriptionID: sub.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) == 0 || items[0].Status != model.StatusDeadLetter {
		t.Fatalf("want dead_letter, got %+v", items)
	}
}

func TestSignDeterministic(t *testing.T) {
	a := Sign("secret", []byte("body"))
	b := Sign("secret", []byte("body"))
	if a != b {
		t.Fatal("signing should be deterministic for same inputs")
	}
	if a == Sign("other", []byte("body")) {
		t.Fatal("different secrets should produce different signatures")
	}
}
