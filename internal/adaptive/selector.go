package adaptive

import (
	"context"
	"errors"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/transport"
)

// Selector remembers the chosen transport per destination hostname and keeps
// dial measurements to inform choices for destinations seen for the first time.
type Selector struct {
	mu        sync.RWMutex
	transports []transport.Dialer
	affinity  map[string]string
	stats     map[string]*metric
}

type metric struct {
	attempts uint64
	successes uint64
	failures uint64
	totalLatency time.Duration
	lastLatency time.Duration
}

// Route describes the transport chosen for a successful connection.
type Route struct {
	Destination string
	Transport string
	Latency time.Duration
}

func NewSelector(transports ...transport.Dialer) (*Selector, error) {
	if len(transports) == 0 {
		return nil, errors.New("at least one transport is required")
	}
	seen := make(map[string]bool, len(transports))
	for _, t := range transports {
		if t == nil || strings.TrimSpace(t.Name()) == "" {
			return nil, errors.New("transport and transport name must be non-empty")
		}
		if seen[t.Name()] {
			return nil, errors.New("duplicate transport name: " + t.Name())
		}
		seen[t.Name()] = true
	}
	s := &Selector{
		transports: append([]transport.Dialer(nil), transports...),
		affinity: make(map[string]string),
		stats: make(map[string]*metric),
	}
	for _, t := range transports {
		s.stats[t.Name()] = &metric{}
	}
	return s, nil
}

// Dial connects to address. A destination keeps its selected transport until
// that transport fails, at which point the selector clears affinity and retries.
func (s *Selector) Dial(ctx context.Context, network, address string) (net.Conn, Route, error) {
	key := destinationKey(address)
	if key == "" {
		key = strings.ToLower(address)
	}

	s.mu.RLock()
	preferred := s.affinity[key]
	s.mu.RUnlock()

	ordered := s.orderedTransports(preferred)
	var lastErr error
	for _, t := range ordered {
		start := time.Now()
		conn, err := t.DialContext(ctx, network, address)
		latency := time.Since(start)
		s.record(t.Name(), latency, err == nil)
		if err == nil {
			s.mu.Lock()
			s.affinity[key] = t.Name()
			s.mu.Unlock()
			return conn, Route{Destination: key, Transport: t.Name(), Latency: latency}, nil
		}
		lastErr = err
		if preferred != "" && t.Name() == preferred {
			s.mu.Lock()
			if s.affinity[key] == preferred {
				delete(s.affinity, key)
			}
			s.mu.Unlock()
		}
	}
	if lastErr == nil {
		lastErr = errors.New("no transport could be selected")
	}
	return nil, Route{Destination: key}, lastErr
}

func (s *Selector) orderedTransports(preferred string) []transport.Dialer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := append([]transport.Dialer(nil), s.transports...)
	sort.SliceStable(out, func(i, j int) bool {
		if i == j { return false }
		ni, nj := out[i].Name(), out[j].Name()
		if ni == preferred { return true }
		if nj == preferred { return false }
		mi, mj := s.stats[ni], s.stats[nj]
		if mi.successes == 0 && mj.successes > 0 { return true }
		if mj.successes == 0 && mi.successes > 0 { return false }
		if mi.successes == 0 && mj.successes == 0 { return ni < nj }
		ai := float64(mi.totalLatency) / float64(mi.successes)
		aj := float64(mj.totalLatency) / float64(mj.successes)
		fi := float64(mi.failures+1) / float64(mi.attempts+2)
		fj := float64(mj.failures+1) / float64(mj.attempts+2)
		return ai*(1+fi) < aj*(1+fj)
	})
	return out
}

func (s *Selector) record(name string, latency time.Duration, success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.stats[name]
	m.attempts++
	m.lastLatency = latency
	if success {
		m.successes++
		m.totalLatency += latency
	} else {
		m.failures++
	}
}

// Observations returns a stable snapshot of measurements for diagnostics/tests.
func (s *Selector) Observations() []transport.Observation {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]transport.Observation, 0, len(s.stats))
	for name, m := range s.stats {
		o := transport.Observation{
			Transport: name, Attempts: m.attempts, Successes: m.successes,
			Failures: m.failures, LastLatency: m.lastLatency,
		}
		if m.successes > 0 {
			o.AvgLatency = time.Duration(int64(m.totalLatency) / int64(m.successes))
		}
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Transport < out[j].Transport })
	return out
}

func destinationKey(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil { return "" }
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if ip := net.ParseIP(host); ip != nil { return ip.String() }
	return host
}
