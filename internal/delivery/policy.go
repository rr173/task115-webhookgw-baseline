package delivery

import (
	"time"
	"webhookgw/internal/model"
)

func NextRetry(policy model.RetryPolicy, attempt int, now time.Time) time.Time {
	return now.Add(policy.Delay(attempt))
}

// ShouldRetry reports whether an HTTP response status indicates a transient
// failure that should consume a retry attempt under the subscription's retry
// policy rather than be moved straight to the dead-letter queue: 408 (Request
// Timeout), 425 (Too Early), 429 (Too Many Requests), and any 5xx. A 425 tells
// the client the request was sent too early and should be retried later, so it
// must not be treated as a permanent failure.
func ShouldRetry(code int) bool { return code == 408 || code == 425 || code == 429 || code >= 500 }
