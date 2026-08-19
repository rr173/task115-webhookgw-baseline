package attempt

import "webhookgw/internal/model"

type Summary struct {
	Total     int `json:"total"`
	Pending   int `json:"pending"`
	Delivered int `json:"delivered"`
	Failed    int `json:"failed"`
	Dead      int `json:"dead"`
}

func Summarize(rows []*model.Attempt) Summary {
	out := Summary{Total: len(rows)}
	for _, row := range rows {
		if row == nil {
			continue
		}
		switch row.Status {
		case model.StatusPending, model.StatusInFlight:
			out.Pending++
		case model.StatusDelivered:
			out.Delivered++
		case model.StatusFailed:
			out.Failed++
		case model.StatusDeadLetter:
			out.Dead++
		}
	}
	return out
}
