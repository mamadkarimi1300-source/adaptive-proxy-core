package transport

import (
	"context"
	"net"
	"time"
)

// TCP is the default direct transport.
type TCP struct {
	DialTimeout time.Duration
}

func (t TCP) Name() string { return "tcp" }

func (t TCP) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	timeout := t.DialTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	d := net.Dialer{Timeout: timeout, KeepAlive: 30 * time.Second}
	return d.DialContext(ctx, network, address)
}
