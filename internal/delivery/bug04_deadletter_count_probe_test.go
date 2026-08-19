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

func TestBug04_ReplayKeepsTheDeadLetterAttemptHistory(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(400, 0))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) }))
	defer srv.Close()
	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	sub, err := subs.Create("dead", srv.URL, nil, 0, model.RetryPolicy{MaxAttempts: 1, BaseDelay: time.Second, MaxDelay: time.Second})
	if err != nil { t.Fatal(err) }
	m := New(st, subs, metrics.New(), clk, srv.Client())
	ev := &model.Event{ID: "dead-event", Type: "payment.failed", Payload: `{}`, CreatedAt: clk.Now()}
	if err := st.SaveEvent(ev); err != nil { t.Fatal(err) }
	if _, err := m.Enqueue(ev); err != nil { t.Fatal(err) }
	if err := m.Dispatch(context.Background()); err != nil { t.Fatal(err) }
	service := attempt.New(st)
	dls, err := service.DeadLetters()
	if err != nil { t.Fatal(err) }
	if len(dls) != 1 || dls[0].AttemptCount != 1 { t.Fatalf("dead-letter history = %+v, want count 1", dls) }
	if err := service.ReplayDeadLetter(dls[0].AttemptID); err != nil { t.Fatal(err) }
	replayed, err := service.Get(dls[0].AttemptID)
	if err != nil { t.Fatal(err) }
	if replayed == nil || replayed.AttemptCount != 1 { t.Fatalf("replayed attempt = %+v, want preserved count 1", replayed) }
	if replayed.SubscriptionID != sub.ID { t.Fatalf("replayed subscription = %q", replayed.SubscriptionID) }
}
