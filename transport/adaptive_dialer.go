package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	minThroughputSampleBytes = 512 * 1024
	minThroughputSampleSpan  = 250 * time.Millisecond
	megabitsPerSecond        = 1_000_000.0
)

// Candidate is one configured outbound transport/path.
type Candidate struct {
	Name   string
	Dialer Dialer
}

type candidateStats struct {
	attempts     uint64
	samples      uint64
	throughputMbps float64
	failures     uint64
}

// AdaptiveDialer chooses paths using measured goodput from real application traffic.
// It never sends synthetic payloads to arbitrary public destinations.
type AdaptiveDialer struct {
	mu          sync.Mutex
	candidates  []Candidate
	stats       map[string]candidateStats
	nextExplore int
	alpha       float64
}

func NewAdaptiveDialer(candidates ...Candidate) (*AdaptiveDialer, error) {
	if len(candidates) == 0 {
		return nil, errors.New("adaptive dialer requires at least one candidate")
	}
	seen := make(map[string]bool, len(candidates))
	for _, candidate := range candidates {
		if candidate.Name == "" || candidate.Dialer == nil {
			return nil, errors.New("adaptive dialer candidates require a name and dialer")
		}
		if seen[candidate.Name] {
			return nil, errors.New("adaptive dialer candidate names must be unique")
		}
		seen[candidate.Name] = true
	}
	return &AdaptiveDialer{
		candidates: append([]Candidate(nil), candidates...),
		stats:      make(map[string]candidateStats, len(candidates)),
		alpha:      0.25,
	}, nil
}

// DialContext picks an unmeasured candidate for bounded exploration, then the
// highest observed EWMA throughput. If dialing fails, remaining paths are tried.
func (d *AdaptiveDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ordered := d.orderedCandidates()
	var lastErr error
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := candidate.Dialer.DialContext(ctx, network, address)
		d.recordAttempt(candidate.Name, err == nil)
		if err != nil {
			lastErr = fmt.Errorf("%s path: %w", candidate.Name, err)
			continue
		}
		return &throughputConn{
			Conn: conn,
			onClose: func(bytes uint64, span time.Duration) {
				d.recordThroughput(candidate.Name, bytes, span)
			},
		}, nil
	}
	return nil, lastErr
}

func (d *AdaptiveDialer) orderedCandidates() []Candidate {
	d.mu.Lock()
	defer d.mu.Unlock()

	ordered := append([]Candidate(nil), d.candidates...)
	// Try each candidate at least once and give unmeasured candidates up to
	// three attempts to produce enough real traffic for a goodput sample.
	for i := 0; i < len(ordered); i++ {
		idx := (d.nextExplore + i) % len(ordered)
		stats := d.stats[ordered[idx].Name]
		if stats.samples == 0 && stats.attempts < 3 {
			d.nextExplore = (idx + 1) % len(ordered)
			first := ordered[idx]
			ordered = append(ordered[:idx], ordered[idx+1:]...)
			return append([]Candidate{first}, d.rank(ordered)...)
		}
	}
	return d.rank(ordered)
}

func (d *AdaptiveDialer) rank(candidates []Candidate) []Candidate {
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && d.score(d.stats[candidates[j].Name]) > d.score(d.stats[candidates[j-1].Name]); j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}
	return candidates
}

func (d *AdaptiveDialer) score(stats candidateStats) float64 {
	if stats.samples == 0 {
		return -1 // no valid sample: explore rather than inventing a speed
	}
	// Repeated failures reduce preference; successful throughput samples heal the penalty.
	return stats.throughputMbps / (1 + float64(stats.failures))
}

func (d *AdaptiveDialer) recordAttempt(name string, success bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	stats := d.stats[name]
	stats.attempts++
	if !success {
		stats.failures++
	} else if stats.failures > 0 {
		stats.failures--
	}
	d.stats[name] = stats
}

func (d *AdaptiveDialer) recordThroughput(name string, bytes uint64, span time.Duration) {
	if bytes < minThroughputSampleBytes || span < minThroughputSampleSpan {
		return
	}
	mbps := float64(bytes) * 8 / span.Seconds() / megabitsPerSecond
	if mbps <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	stats := d.stats[name]
	if stats.samples == 0 {
		stats.throughputMbps = mbps
	} else {
		stats.throughputMbps = d.alpha*mbps + (1-d.alpha)*stats.throughputMbps
	}
	stats.samples++
	if stats.failures > 0 {
		stats.failures--
	}
	d.stats[name] = stats
}

// Snapshot returns measured EWMA application goodput in megabits per second.
// Paths without enough real traffic are omitted.
func (d *AdaptiveDialer) Snapshot() map[string]float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(map[string]float64, len(d.stats))
	for name, stats := range d.stats {
		if stats.samples > 0 {
			result[name] = stats.throughputMbps
		}
	}
	return result
}

type throughputConn struct {
	net.Conn
	mu      sync.Mutex
	bytes   uint64
	first   time.Time
	last    time.Time
	onClose func(uint64, time.Duration)
	once    sync.Once
}

func (c *throughputConn) account(n int) {
	if n <= 0 { return }
	now := time.Now()
	c.mu.Lock()
	if c.first.IsZero() { c.first = now }
	c.last = now
	c.bytes += uint64(n)
	c.mu.Unlock()
}

func (c *throughputConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.account(n)
	return n, err
}

func (c *throughputConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	c.account(n)
	return n, err
}

// CloseWrite preserves half-close support when the underlying transport exposes it.
func (c *throughputConn) CloseWrite() error {
	if cw, ok := c.Conn.(interface{ CloseWrite() error }); ok {
		return cw.CloseWrite()
	}
	return errors.New("underlying connection does not support CloseWrite")
}

func (c *throughputConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		c.mu.Lock()
		bytes, first, last := c.bytes, c.first, c.last
		c.mu.Unlock()
		if !first.IsZero() && !last.Before(first) && c.onClose != nil {
			c.onClose(bytes, last.Sub(first))
		}
	})
	return err
}
