package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"webhookgw/internal/model"

	"github.com/google/uuid"
)

type createSubRequest struct {
	Name      string      `json:"name"`
	Endpoint  string      `json:"endpoint"`
	Events    []string    `json:"events"`
	RateLimit int         `json:"rate_limit"`
	Retry     retryPolicy `json:"retry"`
}

type retryPolicy struct {
	MaxAttempts int `json:"max_attempts"`
	BaseDelayMs int `json:"base_delay_ms"`
	MaxDelayMs  int `json:"max_delay_ms"`
}

type eventRequest struct {
	Type    string `json:"type"`
	Payload string `json:"payload"`
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, v interface{}) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	return dec.Decode(v)
}

func (a *API) createSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	sub, err := a.subs.Create(req.Name, req.Endpoint, req.Events, req.RateLimit, model.RetryPolicy{
		MaxAttempts: req.Retry.MaxAttempts,
		BaseDelay:   time.Duration(req.Retry.BaseDelayMs) * time.Millisecond,
		MaxDelay:    time.Duration(req.Retry.MaxDelayMs) * time.Millisecond,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, sub)
}

func (a *API) listSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := a.subs.List()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if subs == nil {
		subs = []*model.Subscription{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"subscriptions": subs, "count": len(subs)})
}

func (a *API) getSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sub, err := a.subs.Get(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if sub == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "subscription not found"})
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (a *API) updateSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req createSubRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	sub, err := a.subs.Update(id, req.Name, req.Endpoint, req.Events, req.RateLimit, model.RetryPolicy{
		MaxAttempts: req.Retry.MaxAttempts,
		BaseDelay:   time.Duration(req.Retry.BaseDelayMs) * time.Millisecond,
		MaxDelay:    time.Duration(req.Retry.MaxDelayMs) * time.Millisecond,
	})
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (a *API) deleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.subs.Delete(id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (a *API) rotateSecret(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sub, err := a.subs.RotateSecret(id)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (a *API) subscriptionStats(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	by, total, err := a.attempts.Stats(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"subscription_id": id, "total": total, "by_status": by})
}

func (a *API) subscriptionDeliveries(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	items, total, err := a.attempts.List(model.AttemptFilter{SubscriptionID: id, Page: 1, PageSize: 50})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"attempts": items, "total": total})
}

func (a *API) testSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sub, err := a.subs.Get(id)
	if err != nil || sub == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "subscription not found"})
		return
	}
	ev := &model.Event{ID: uuid.NewString(), Type: "test.ping", Payload: `{"probe":true}`, CreatedAt: time.Now()}
	if err := a.saveEvent(ev); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if _, err := a.delivery.Enqueue(ev); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if err := a.delivery.Dispatch(r.Context()); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	items, _, err := a.attempts.List(model.AttemptFilter{SubscriptionID: id, Page: 1, PageSize: 1})
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	status := ""
	if len(items) > 0 {
		status = string(items[0].Status)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": status})
}

func (a *API) ingestEvent(w http.ResponseWriter, r *http.Request) {
	var req eventRequest
	if err := readJSON(r, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	if strings.TrimSpace(req.Type) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "event type is required"})
		return
	}
	ev := &model.Event{ID: uuid.NewString(), Type: req.Type, Payload: req.Payload, CreatedAt: time.Now()}
	if err := a.saveEvent(ev); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	n, err := a.delivery.Enqueue(ev)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]interface{}{"event_id": ev.ID, "attempts_created": n})
}

func (a *API) listEvents(w http.ResponseWriter, r *http.Request) {
	limit := queryInt(r, "limit", 100)
	events, err := a.listEventsStore(limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if events == nil {
		events = []*model.Event{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"events": events, "count": len(events)})
}

func (a *API) listAttempts(w http.ResponseWriter, r *http.Request) {
	f := model.AttemptFilter{
		SubscriptionID: r.URL.Query().Get("subscription_id"),
		EventID:        r.URL.Query().Get("event_id"),
		Status:         model.AttemptStatus(r.URL.Query().Get("status")),
		Page:           queryInt(r, "page", 1),
		PageSize:       queryInt(r, "page_size", 20),
	}
	items, total, err := a.attempts.List(f)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if items == nil {
		items = []*model.Attempt{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"attempts": items, "total": total, "page": f.Page, "page_size": f.PageSize})
}

func (a *API) getAttempt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a2, err := a.attempts.Get(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if a2 == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "attempt not found"})
		return
	}
	writeJSON(w, http.StatusOK, a2)
}

func (a *API) retryAttempt(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	a2, err := a.attempts.Retry(id)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if a2 == nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "attempt not found"})
		return
	}
	writeJSON(w, http.StatusOK, a2)
}

func (a *API) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	items, err := a.attempts.DeadLetters()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if items == nil {
		items = []*model.DeadLetter{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"dead_letters": items, "count": len(items)})
}

func (a *API) replayDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.attempts.ReplayDeadLetter(id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "requeued"})
}

func (a *API) deleteDeadLetter(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := a.attempts.DeleteDeadLetter(id); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (a *API) dispatch(w http.ResponseWriter, r *http.Request) {
	if err := a.delivery.Dispatch(context.Background()); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "dispatched"})
}

func (a *API) dispatchPlan(w http.ResponseWriter, r *http.Request) {
	horizonMS := queryInt(r, "horizon_ms", 0)
	limit := queryInt(r, "limit", 100)
	plan, err := a.delivery.Plan(r.Context(), time.Duration(horizonMS)*time.Millisecond, limit)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (a *API) metricsView(w http.ResponseWriter, r *http.Request) {
	total, subs := a.metrics.Snapshot()
	writeJSON(w, http.StatusOK, map[string]interface{}{"total": total, "per_subscription": subs})
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) version(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"version": Version})
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

// saveEvent and listEventsStore delegate to the store via the attempt service's
// underlying store; they are kept here to avoid exporting the store from the API.
func (a *API) saveEvent(ev *model.Event) error {
	return a.delivery.SaveEvent(ev)
}

func (a *API) listEventsStore(limit int) ([]*model.Event, error) {
	return a.delivery.ListEvents(limit)
}
