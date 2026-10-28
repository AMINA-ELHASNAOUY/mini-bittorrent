package seed

import (
	"sync"
	"time"
)

// rateLimiter is a token bucket shared by every connection: tokens (bytes)
// refill at `rate` per second, and sending n bytes waits until n are available.
type rateLimiter struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newRateLimiter(bytesPerSecond int64) *rateLimiter {
	burst := float64(bytesPerSecond) / 4 // a quarter second of traffic
	if burst < 128<<10 {
		burst = 128 << 10 // must fit the largest block we serve
	}
	return &rateLimiter{rate: float64(bytesPerSecond), burst: burst, tokens: burst, last: time.Now()}
}

func (l *rateLimiter) wait(n int) {
	l.mu.Lock()
	now := time.Now()
	l.tokens += now.Sub(l.last).Seconds() * l.rate
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.last = now
	l.tokens -= float64(n) // may go negative: the debt is paid by sleeping
	var sleep time.Duration
	if l.tokens < 0 {
		sleep = time.Duration(-l.tokens / l.rate * float64(time.Second))
	}
	l.mu.Unlock()
	if sleep > 0 {
		time.Sleep(sleep)
	}
}
