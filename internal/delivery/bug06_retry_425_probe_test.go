package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"webhookgw/internal/attempt"
	"webhookgw/internal/clock"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/subscription"
)

func TestBug06_TooEarlyResponseUsesTheConfiguredRetryBudget(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(600, 0))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusTooEarly) }))
	defer srv.Close()
	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	sub, err := subs.Create("early", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Second})
	if err != nil { t.Fatal(err) }
	m := New(st, subs, metrics.New(), clk, srv.Client())
	ev := &model.Event{ID: "early-event", Type: "invoice.pending", Payload: `{}`, CreatedAt: clk.Now()}
	if err := st.SaveEvent(ev); err != nil { t.Fatal(err) }
	if _, err := m.Enqueue(ev); err != nil { t.Fatal(err) }
	if err := m.Dispatch(context.Background()); err != nil { t.Fatal(err) }
	items, _, err := attempt.New(st).List(model.AttemptFilter{SubscriptionID: sub.ID})
	if err != nil { t.Fatal(err) }
	if len(items) != 1 || items[0].Status != model.StatusPending || items[0].AttemptCount != 1 { t.Fatalf("425 delivery = %+v, want pending retry", items) }
}
