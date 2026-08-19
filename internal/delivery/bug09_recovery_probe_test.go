package delivery

import (
	"testing"
	"time"

	"webhookgw/internal/clock"
	"webhookgw/internal/metrics"
	"webhookgw/internal/model"
	"webhookgw/internal/subscription"
)

func TestBug09_RestartRecoversPersistedInFlightAttempt(t *testing.T) {
	clk := clock.NewFakeClock(time.Unix(900, 0))
	st := openTestStore(t)
	defer st.Close()
	a := &model.Attempt{ID: "inflight", SubscriptionID: "sub", EventID: "event", EventType: "document.ready", Payload: `{}`, Status: model.StatusInFlight, AttemptCount: 1, NextAttemptAt: clk.Now().Add(time.Hour), CreatedAt: clk.Now(), UpdatedAt: clk.Now()}
	if err := st.SaveAttempt(a); err != nil { t.Fatal(err) }
	m := New(st, subscription.New(st, clk), metrics.New(), clk, nil)
	n, err := m.Recover()
	if err != nil { t.Fatal(err) }
	if n != 1 { t.Fatalf("recovered %d attempts, want 1", n) }
	got, err := st.GetAttempt(a.ID)
	if err != nil { t.Fatal(err) }
	if got == nil || got.Status != model.StatusPending || !got.NextAttemptAt.Equal(clk.Now()) { t.Fatalf("recovered attempt = %+v", got) }
}
