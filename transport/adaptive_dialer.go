package transport

import (
	"context"
	"errors"
	"fmt"
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
	attempts uint64
	samples  uint64
	ewma     time.Duration
	failures uint64
}

// AdaptiveDialer ranks configured paths by observed connection setup time.
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
		stats: make(map[string]candidateStats, len(candidates)),
		alpha: 0.25,
	}, nil
}

// DialContext explores unmeasured paths, then prefers the lowest EWMA latency.
// If a path fails, it tries the remaining paths before returning an error.
func (d *AdaptiveDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	ordered := d.orderedCandidates()
	var lastErr error
	for _, candidate := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		started := time.Now()
		conn, err := candidate.Dialer.DialContext(ctx, network, address)
		d.record(candidate.Name, time.Since(started), err == nil)
		if err == nil {
			return conn, nil
		}
		lastErr = fmt.Errorf("%s path: %w", candidate.Name, err)
	}
	return nil, lastErr
}

func (d *AdaptiveDialer) orderedCandidates() []Candidate {
	d.mu.Lock()
	defer d.mu.Unlock()

	ordered := append([]Candidate(nil), d.candidates...)
	// Explore one unseen path first; otherwise rank by measured latency plus failure penalty.
	for i := 0; i < len(ordered); i++ {
		idx := (d.nextExplore + i) % len(ordered)
		if d.stats[ordered[idx].Name].attempts == 0 {
			d.nextExplore = (idx + 1) % len(ordered)
			first := ordered[idx]
			ordered = append(ordered[:idx], ordered[idx+1:]...)
			return append([]Candidate{first}, d.rank(ordered)...)
		}
	}
	return d.rank(ordered)
}

func (d *AdaptiveDialer) rank(candidates []Candidate) []Candidate {
	// Stable insertion sort keeps tie behavior deterministic.
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && d.score(d.stats[candidates[j].Name]) < d.score(d.stats[candidates[j-1].Name]); j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}
	return candidates
}

func (d *AdaptiveDialer) score(stats candidateStats) time.Duration {
	penalty := time.Duration(stats.failures) * time.Second
	if penalty > 30*time.Second { penalty = 30*time.Second }
	return stats.ewma + penalty
}

func (d *AdaptiveDialer) record(name string, elapsed time.Duration, success bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	stats := d.stats[name]
	stats.attempts++
	if !success {
		stats.failures++
		d.stats[name] = stats
		return
	}
	if stats.samples == 0 {
		stats.ewma = elapsed
	} else {
		stats.ewma = time.Duration(d.alpha*float64(elapsed) + (1-d.alpha)*float64(stats.ewma))
	}
	stats.samples++
	if stats.failures > 0 { stats.failures-- }
	d.stats[name] = stats
}

// Snapshot returns observed EWMA connection setup times, keyed by candidate name.
func (d *AdaptiveDialer) Snapshot() map[string]time.Duration {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(map[string]time.Duration, len(d.stats))
	for name, stats := range d.stats {
		if stats.samples > 0 { result[name] = stats.ewma }
	}
	return result
}
