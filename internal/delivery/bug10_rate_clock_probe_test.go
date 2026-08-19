package delivery

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"webhookgw/internal/clock"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/subscription"
)

func TestBug10_RateWindowFollowsTheDeliveryClock(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(1000, 0))
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { atomic.AddInt32(&calls, 1); w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	if _, err := subs.Create("limited", srv.URL, nil, 1, model.RetryPolicy{}); err != nil { t.Fatal(err) }
	m := New(st, subs, metrics.New(), clk, srv.Client())
	for _, id := range []string{"first", "second"} {
		ev := &model.Event{ID: id, Type: "stock.changed", Payload: `{}`, CreatedAt: clk.Now()}
		if err := st.SaveEvent(ev); err != nil { t.Fatal(err) }
		if _, err := m.Enqueue(ev); err != nil { t.Fatal(err) }
		if err := m.Dispatch(context.Background()); err != nil { t.Fatal(err) }
		clk.Advance(time.Second)
	}
	if got := atomic.LoadInt32(&calls); got != 2 { t.Fatalf("receiver calls = %d, want 2 after one logical second", got) }
}
