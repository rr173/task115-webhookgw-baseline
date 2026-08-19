package store

import (
	"testing"
	"time"

	"webhookgw/internal/model"
)

func TestBug05_FirstAttemptPageStartsWithMostRecentDelivery(t *testing.T) {
	st := mustOpen(t)
	defer st.Close()
	base := time.Unix(500, 0)
	for i, id := range []string{"oldest", "middle", "newest"} {
		now := base.Add(time.Duration(i) * time.Second)
		a := &model.Attempt{ID: id, SubscriptionID: "sub", EventID: id, EventType: "shipment.updated", Payload: `{}`, Status: model.StatusDelivered, AttemptCount: 1, NextAttemptAt: now, CreatedAt: now, UpdatedAt: now}
		if err := st.SaveAttempt(a); err != nil { t.Fatal(err) }
	}
	items, total, err := st.ListAttempts(model.AttemptFilter{SubscriptionID: "sub", Page: 1, PageSize: 1})
	if err != nil { t.Fatal(err) }
	if total != 3 || len(items) != 1 || items[0].ID != "newest" { t.Fatalf("page one = %+v total=%d, want newest only", items, total) }
}
