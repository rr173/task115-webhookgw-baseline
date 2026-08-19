package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"webhookgw/internal/clock"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/subscription"
)

func TestBug07_DeadLetterCountersStaySeparateFromRetryFailures(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(700, 0))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusBadRequest) }))
	defer srv.Close()
	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	if _, err := subs.Create("metrics", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 2, BaseDelay: time.Second, MaxDelay: time.Second}); err != nil { t.Fatal(err) }
	mtr := metrics.New()
	m := New(st, subs, mtr, clk, srv.Client())
	ev := &model.Event{ID: "metrics-event", Type: "profile.invalid", Payload: `{}`, CreatedAt: clk.Now()}
	if err := st.SaveEvent(ev); err != nil { t.Fatal(err) }
	if _, err := m.Enqueue(ev); err != nil { t.Fatal(err) }
	if err := m.Dispatch(context.Background()); err != nil { t.Fatal(err) }
	total, _ := mtr.Snapshot()
	if total.DeadLettered != 1 || total.Failed != 0 { t.Fatalf("counters = %+v, want one dead letter and no retry failure", total) }
}
