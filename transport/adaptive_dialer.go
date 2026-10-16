package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

// Candidate is one configured outbound transport/path.
type Candidate struct {
	Name   string
	Dialer Dialer
}

type candidateStats struct {
	samples  uint64
	ewma     time.Duration
	failures uint64
}

// AdaptiveDialer selects among configured dialers using observed connection setup time.
// It explores candidates with no successful samples before preferring the lowest EWMA.
type AdaptiveDialer struct {
	mu          sync.Mutex
	candidates  []Candidate
	stats       map[string]candidateStats
	nextExplore int
	alpha       float64
}

// NewAdaptiveDialer creates an adaptive selector. Candidate names must be unique and dialers non-nil.
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

// DialContext selects a candidate, records its connection setup time, and returns its connection.
// Measurements are from real dial attempts; this method does not generate synthetic traffic.
func (d *AdaptiveDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	candidate := d.selectCandidate()
	started := time.Now()
	conn, err := candidate.Dialer.DialContext(ctx, network, address)
	d.record(candidate.Name, time.Since(started), err == nil)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func (d *AdaptiveDialer) selectCandidate() Candidate {
	d.mu.Lock()
	defer d.mu.Unlock()

	for i := 0; i < len(d.candidates); i++ {
		idx := (d.nextExplore + i) % len(d.candidates)
		if d.stats[d.candidates[idx].Name].samples == 0 {
			d.nextExplore = (idx + 1) % len(d.candidates)
			return d.candidates[idx]
		}
	}

	best := d.candidates[0]
	bestScore := d.score(d.stats[best.Name])
	for _, candidate := range d.candidates[1:] {
		score := d.score(d.stats[candidate.Name])
		if score < bestScore {
			best, bestScore = candidate, score
		}
	}
	return best
}

func (d *AdaptiveDialer) score(stats candidateStats) time.Duration {
	// Failed attempts receive a penalty so a repeatedly failing path is not preferred.
	penalty := time.Duration(stats.failures) * time.Second
	if stats.failures > 30 {
		penalty = 30 * time.Second
	}
	return stats.ewma + penalty
}

func (d *AdaptiveDialer) record(name string, elapsed time.Duration, success bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	stats := d.stats[name]
	if !success {
		stats.failures++
		// A failed attempt is represented by a bounded penalty without treating it as a latency sample.
		d.stats[name] = stats
		return
	}
	if stats.samples == 0 {
		stats.ewma = elapsed
	} else {
		stats.ewma = time.Duration(d.alpha*float64(elapsed) + (1-d.alpha)*float64(stats.ewma))
	}
	stats.samples++
	if stats.failures > 0 {
		stats.failures--
	}
	d.stats[name] = stats
}

// Snapshot returns a copy of the current latency observations, keyed by candidate name.
func (d *AdaptiveDialer) Snapshot() map[string]time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(map[string]time.Duration, len(d.stats))
	for name, stats := range d.stats {
		if stats.samples > 0 {
			result[name] = stats.ewma
		}
	}
	return result
}
