package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"webhookgw/internal/attempt"
	"webhookgw/internal/clock"
	"webhookgw/internal/delivery"
	"webhookgw/internal/metrics"
	"webhookgw/internal/store"
	"webhookgw/internal/subscription"
)

func mustOpen(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestHTTPEndpoints(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	subs := subscription.New(st, clock.RealClock{})
	m := metrics.New()
	d := delivery.New(st, subs, m, clock.RealClock{}, http.DefaultClient)
	att := attempt.New(st)
	srv := httptest.NewServer(NewMux(subs, d, att, m))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/subscriptions", "application/json",
		strings.NewReader(`{"name":"s","endpoint":"https://example.com/h","events":[],"rate_limit":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 201 {
		t.Fatalf("create status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, err = http.Get(srv.URL + "/subscriptions")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("list status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(srv.URL + "/health")
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(body), "ok") {
		t.Fatalf("health = %d %s", resp.StatusCode, body)
	}

	resp, _ = http.Get(srv.URL + "/version")
	if resp.StatusCode != 200 {
		t.Fatalf("version = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp, _ = http.Get(srv.URL + "/metrics")
	if resp.StatusCode != 200 {
		t.Fatalf("metrics = %d", resp.StatusCode)
	}
	resp.Body.Close()
}
