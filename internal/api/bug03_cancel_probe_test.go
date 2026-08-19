package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"webhookgw/internal/attempt"
	"webhookgw/internal/clock"
	"webhookgw/internal/delivery"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/subscription"
)

func TestBug03_CancelledDispatchDoesNotStartWebhookDelivery(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	called := make(chan struct{}, 1)
	receiver := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { called <- struct{}{}; w.WriteHeader(http.StatusOK) }))
	defer receiver.Close()
	clk := clock.NewFakeClock(time.Unix(300, 0))
	subs := subscription.New(st, clk)
	if _, err := subs.Create("cancel", receiver.URL, nil, 0, model.RetryPolicy{}); err != nil { t.Fatal(err) }
	d := delivery.New(st, subs, metrics.New(), clk, receiver.Client())
	if err := st.SaveEvent(&model.Event{ID: "cancel-event", Type: "order.cancelled", Payload: `{}`, CreatedAt: clk.Now()}); err != nil { t.Fatal(err) }
	if _, err := d.Enqueue(&model.Event{ID: "cancel-event", Type: "order.cancelled", Payload: `{}`, CreatedAt: clk.Now()}); err != nil { t.Fatal(err) }
	a := &API{subs: subs, delivery: d, attempts: attempt.New(st), metrics: metrics.New()}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	req := httptest.NewRequest(http.MethodPost, "/dispatch", nil).WithContext(ctx)
	res := httptest.NewRecorder()
	a.dispatch(res, req)
	if res.Code != http.StatusInternalServerError { t.Fatalf("cancelled dispatch status = %d, want 500", res.Code) }
	select { case <-called: t.Fatal("cancelled dispatch contacted receiver"); case <-time.After(100 * time.Millisecond): }
}
