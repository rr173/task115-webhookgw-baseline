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

func TestBug08_RotatedSecretSignsTheNextDelivery(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(800, 0))
	var expected string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("X-Webhook-Signature"); got != expected { t.Errorf("signature = %q, want %q", got, expected) }
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	st := openTestStore(t)
	defer st.Close()
	subs := subscription.New(st, clk)
	sub, err := subs.Create("billing", srv.URL, nil, 0, model.RetryPolicy{})
	if err != nil { t.Fatal(err) }
	rotated, err := subs.RotateSecret(sub.ID)
	if err != nil { t.Fatal(err) }
	expected = Sign(rotated.SigningSecret, []byte(`{"paid":true}`))
	m := New(st, subs, metrics.New(), clk, srv.Client())
	ev := &model.Event{ID: "secret-event", Type: "invoice.paid", Payload: `{"paid":true}`, CreatedAt: clk.Now()}
	if err := st.SaveEvent(ev); err != nil { t.Fatal(err) }
	if _, err := m.Enqueue(ev); err != nil { t.Fatal(err) }
	if err := m.Dispatch(context.Background()); err != nil { t.Fatal(err) }
}
