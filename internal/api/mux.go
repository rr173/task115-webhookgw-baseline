// Package api exposes the HTTP surface of the webhook delivery gateway:
// subscription management, event ingestion, attempt inspection, dead-letter
// operations, manual dispatch and metrics.
package api

import (
	"net/http"

	"webhookgw/internal/attempt"
	"webhookgw/internal/delivery"
	"webhookgw/internal/metrics"
	"webhookgw/internal/subscription"
)

// Version identifies the running service build.
const Version = "task115-webhookgw/1.0.0"

// API wires the HTTP handlers to the underlying services.
type API struct {
	subs     *subscription.Service
	delivery *delivery.Manager
	attempts *attempt.Service
	metrics  *metrics.Metrics
}

// NewMux builds the HTTP handler with all routes registered. It uses Go 1.22+
// method-based ServeMux patterns.
func NewMux(subs *subscription.Service, d *delivery.Manager, att *attempt.Service, m *metrics.Metrics) http.Handler {
	api := &API{subs: subs, delivery: d, attempts: att, metrics: m}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /subscriptions", api.createSubscription)
	mux.HandleFunc("GET /subscriptions", api.listSubscriptions)
	mux.HandleFunc("GET /subscriptions/{id}", api.getSubscription)
	mux.HandleFunc("PUT /subscriptions/{id}", api.updateSubscription)
	mux.HandleFunc("DELETE /subscriptions/{id}", api.deleteSubscription)
	mux.HandleFunc("POST /subscriptions/{id}/rotate-secret", api.rotateSecret)
	mux.HandleFunc("GET /subscriptions/{id}/stats", api.subscriptionStats)
	mux.HandleFunc("GET /subscriptions/{id}/deliveries", api.subscriptionDeliveries)
	mux.HandleFunc("POST /subscriptions/{id}/test", api.testSubscription)
	mux.HandleFunc("POST /events", api.ingestEvent)
	mux.HandleFunc("GET /events", api.listEvents)
	mux.HandleFunc("GET /attempts", api.listAttempts)
	mux.HandleFunc("GET /attempts/{id}", api.getAttempt)
	mux.HandleFunc("POST /attempts/{id}/retry", api.retryAttempt)
	mux.HandleFunc("GET /deadletters", api.listDeadLetters)
	mux.HandleFunc("POST /deadletters/{id}/replay", api.replayDeadLetter)
	mux.HandleFunc("DELETE /deadletters/{id}", api.deleteDeadLetter)
	mux.HandleFunc("POST /dispatch", api.dispatch)
	mux.HandleFunc("GET /dispatch/plan", api.dispatchPlan)
	mux.HandleFunc("GET /metrics", api.metricsView)
	mux.HandleFunc("GET /diagnostics", api.diagnostics)
	mux.HandleFunc("GET /health", api.health)
	mux.HandleFunc("GET /version", api.version)
	return mux
}
