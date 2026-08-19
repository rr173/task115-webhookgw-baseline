package delivery

import (
	"sync"
	"time"
)

// rateLimiter is a per-subscription token-bucket limiter. A limit of 0 or less
// means unlimited. It is intentionally simple: at most one send per second per
// subscription when a positive limit is configured.
type rateLimiter struct {
	mu    sync.Mutex
	last  map[string]time.Time
	limit map[string]int
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{
		last:  make(map[string]time.Time),
		limit: make(map[string]int),
	}
}

// allow reports whether a send for subID may proceed now under perSec.
func (r *rateLimiter) allow(subID string, perSec int) bool {
	if perSec <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	prev, ok := r.last[subID]
	if !ok || now.Sub(prev) >= time.Second {
		r.last[subID] = now
		return true
	}
	return false
}
