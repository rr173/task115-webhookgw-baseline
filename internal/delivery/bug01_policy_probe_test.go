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

func TestBug01_DefaultRetryPolicySurvivesRegistrationAndLegacyRecovery(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(100, 0))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusInternalServerError) }))
	defer srv.Close()
	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	created, err := subs.Create("registered", srv.URL, nil, 0, model.RetryPolicy{})
	if err != nil { t.Fatal(err) }
	if created.RetryPolicy.MaxAttempts != 5 { t.Fatalf("registered default attempts = %d, want 5", created.RetryPolicy.MaxAttempts) }
	legacy := &model.Subscription{ID: "legacy", Name: "legacy", Endpoint: srv.URL, SigningSecret: "legacy-secret", RetryPolicy: model.RetryPolicy{}, CreatedAt: clk.Now(), UpdatedAt: clk.Now()}
	if err := st.CreateSubscription(legacy); err != nil { t.Fatal(err) }
	m := New(st, subs, metrics.New(), clk, srv.Client())
	ev := &model.Event{ID: "legacy-event", Type: "invoice.failed", Payload: `{}`, CreatedAt: clk.Now()}
	if err := st.SaveEvent(ev); err != nil { t.Fatal(err) }
	if _, err := m.Enqueue(ev); err != nil { t.Fatal(err) }
	if err := m.Dispatch(context.Background()); err != nil { t.Fatal(err) }
	items, _, err := attempt.New(st).List(model.AttemptFilter{SubscriptionID: legacy.ID})
	if err != nil { t.Fatal(err) }
	if len(items) != 1 || items[0].Status != model.StatusPending { t.Fatalf("legacy zero-value policy should schedule a retry, got %+v", items) }
}
