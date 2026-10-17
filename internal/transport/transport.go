package transport

import (
	"context"
	"net"
	"time"
)

// Dialer opens a connection to a destination using one transport.
type Dialer interface {
	Name() string
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}

// Observation is a snapshot of measured dial performance.
type Observation struct {
	Transport   string
	Attempts    uint64
	Successes   uint64
	Failures    uint64
	LastLatency time.Duration
	AvgLatency  time.Duration
}
