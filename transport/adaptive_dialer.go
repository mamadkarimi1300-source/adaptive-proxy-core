package transport

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	minThroughputSampleBytes = 512 * 1024
	minThroughputSampleSpan  = 250 * time.Millisecond
	megabitsPerSecond        = 1_000_000.0

	defaultDestinationRouteTTL = 5 * time.Minute
	maxDestinationRoutes       = 4096
)

// Candidate is one configured outbound transport/path.
type Candidate struct {
	Name   string
	Dialer Dialer
}

type candidateStats struct {
	attempts       uint64
	samples        uint64
	throughputMbps float64
	failures       uint64
}

type routeDecision struct {
	candidateName string
	expiresAt     time.Time
}

// AdaptiveDialer chooses paths using measured goodput from real application traffic.
// Successful decisions are cached per normalized destination, so new connections to
// the same host:port keep using the selected path until the decision expires or fails.
// It never sends synthetic payloads to arbitrary public destinations.
type AdaptiveDialer struct {
	mu          sync.Mutex
	candidates  []Candidate
	stats       map[string]candidateStats
	routes      map[string]routeDecision
	routeTTL    time.Duration
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
		routes:     make(map[string]routeDecision),
		routeTTL:   defaultDestinationRouteTTL,
		alpha:      0.25,
	}, nil
}

// DialContext uses a cached path for a known destination. On a cache miss, it
// selects from candidates ranked by observed application goodput and caches the
// first successful path. If a cached path fails, it is invalidated and fallbacks
// are tried. Existing connections are never moved between transports.
func (d *AdaptiveDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	key := destinationKey(network, address)
	if cached, ok := d.cachedCandidate(key); ok {
		log.Printf("transport route cache hit destination=%s path=%s", address, cached.Name)
		conn, err := d.dialCandidate(ctx, network, address, cached)
		if err == nil {
			return d.wrapConnection(conn, cached.Name), nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.Printf("transport cached path failed; trying fallback destination=%s path=%s error=%v", address, cached.Name, err)
		d.invalidateRoute(key, cached.Name)
		return d.dialFallbacks(ctx, network, address, key, cached.Name)
	}
	return d.dialFallbacks(ctx, network, address, key, "")
}

func (d *AdaptiveDialer) dialFallbacks(ctx context.Context, network, address, key, skipName string) (net.Conn, error) {
	ordered := d.orderedCandidates()
	var lastErr error
	for _, candidate := range ordered {
		if candidate.Name == skipName {
			continue
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		conn, err := d.dialCandidate(ctx, network, address, candidate)
		if err != nil {
			lastErr = fmt.Errorf("%s path: %w", candidate.Name, err)
			continue
		}
		d.cacheRoute(key, candidate.Name)
		return d.wrapConnection(conn, candidate.Name), nil
	}
	if lastErr == nil {
		lastErr = errors.New("no outbound path available")
	}
	return nil, lastErr
}

func (d *AdaptiveDialer) dialCandidate(ctx context.Context, network, address string, candidate Candidate) (net.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	started := time.Now()
	log.Printf("transport dial attempt destination=%s path=%s", address, candidate.Name)
	conn, err := candidate.Dialer.DialContext(ctx, network, address)
	d.recordAttempt(candidate.Name, err == nil)
	if err != nil {
		log.Printf("transport dial failed destination=%s path=%s duration=%s error=%v", address, candidate.Name, time.Since(started).Round(time.Millisecond), err)
		return conn, err
	}
	log.Printf("transport dial succeeded destination=%s path=%s duration=%s", address, candidate.Name, time.Since(started).Round(time.Millisecond))
	return conn, nil
}

func (d *AdaptiveDialer) wrapConnection(conn net.Conn, candidateName string) net.Conn {
	return &throughputConn{
		Conn: conn,
		onClose: func(bytes uint64, span time.Duration) {
			d.recordThroughput(candidateName, bytes, span)
		},
	}
}

func destinationKey(network, address string) string {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return strings.ToLower(address)
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	}
	return network + "|" + net.JoinHostPort(host, port)
}

func (d *AdaptiveDialer) cachedCandidate(key string) (Candidate, bool) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneRoutesLocked(now)
	decision, ok := d.routes[key]
	if !ok || !decision.expiresAt.After(now) {
		delete(d.routes, key)
		return Candidate{}, false
	}
	for _, candidate := range d.candidates {
		if candidate.Name == decision.candidateName {
			return candidate, true
		}
	}
	delete(d.routes, key)
	return Candidate{}, false
}

func (d *AdaptiveDialer) cacheRoute(key, candidateName string) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pruneRoutesLocked(now)
	if _, exists := d.routes[key]; !exists && len(d.routes) >= maxDestinationRoutes {
		var oldestKey string
		var oldest time.Time
		for routeKey, decision := range d.routes {
			if oldest.IsZero() || decision.expiresAt.Before(oldest) {
				oldestKey, oldest = routeKey, decision.expiresAt
			}
		}
		delete(d.routes, oldestKey)
	}
	ttl := d.routeTTL
	if ttl <= 0 {
		ttl = defaultDestinationRouteTTL
	}
	d.routes[key] = routeDecision{candidateName: candidateName, expiresAt: now.Add(ttl)}
}

func (d *AdaptiveDialer) invalidateRoute(key, candidateName string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if decision, ok := d.routes[key]; ok && decision.candidateName == candidateName {
		delete(d.routes, key)
	}
}

func (d *AdaptiveDialer) pruneRoutesLocked(now time.Time) {
	for key, decision := range d.routes {
		if !decision.expiresAt.After(now) {
			delete(d.routes, key)
		}
	}
}

func (d *AdaptiveDialer) orderedCandidates() []Candidate {
	d.mu.Lock()
	defer d.mu.Unlock()

	ordered := append([]Candidate(nil), d.candidates...)
	// Keep rotating paths without a valid goodput sample. Short or idle sessions
	// are not evidence of a path's throughput, so exploration continues until
	// a candidate yields enough real application traffic.
	for i := 0; i < len(ordered); i++ {
		idx := (d.nextExplore + i) % len(ordered)
		stats := d.stats[ordered[idx].Name]
		if stats.samples == 0 {
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
	measuredMbps := stats.throughputMbps
	d.mu.Unlock()
	log.Printf("transport goodput measured path=%s mbps=%.2f sample_bytes=%d span=%s", name, measuredMbps, bytes, span.Round(time.Millisecond))
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
	if n <= 0 {
		return
	}
	now := time.Now()
	c.mu.Lock()
	if c.first.IsZero() {
		c.first = now
	}
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
