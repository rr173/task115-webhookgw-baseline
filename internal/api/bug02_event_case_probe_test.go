package api

import (
	"bytes"
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

func TestBug02_EventTypeCanonicalizationCoversIngressAndStoredEvents(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	clk := clock.NewFakeClock(time.Unix(200, 0))
	subs := subscription.New(st, clk)
	sub, err := subs.Create("orders", "https://example.test/h", []string{"order.created"}, 0, model.RetryPolicy{})
	if err != nil { t.Fatal(err) }
	d := delivery.New(st, subs, metrics.New(), clk, http.DefaultClient)
	srv := httptest.NewServer(NewMux(subs, d, attempt.New(st), metrics.New()))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/events", "application/json", bytes.NewBufferString(`{"type":"Order.Created","payload":"{}"}`))
	if err != nil { t.Fatal(err) }
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted { t.Fatalf("ingest status = %d", resp.StatusCode) }
	events, err := d.ListEvents(10)
	if err != nil { t.Fatal(err) }
	if len(events) != 1 || events[0].Type != "order.created" { t.Fatalf("ingested event type = %+v, want canonical lower-case", events) }
	n, err := d.Enqueue(&model.Event{ID: "legacy-uppercase", Type: "ORDER.CREATED", Payload: `{}`, CreatedAt: clk.Now()})
	if err != nil { t.Fatal(err) }
	if n != 1 { t.Fatalf("legacy event created %d attempts for subscription %s, want 1", n, sub.ID) }
}
