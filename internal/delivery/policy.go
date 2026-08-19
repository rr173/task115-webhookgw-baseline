package delivery

import (
	"time"
	"webhookgw/internal/model"
)

func NextRetry(policy model.RetryPolicy, attempt int, now time.Time) time.Time {
	return now.Add(policy.Delay(attempt))
}
func ShouldRetry(code int) bool { return code == 408 || code == 425 || code == 429 || code >= 500 }
