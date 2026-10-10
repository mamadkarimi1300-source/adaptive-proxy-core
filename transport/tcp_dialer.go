package transport

import (
	"context"
	"net"
	"time"
)

// TCPDialer establishes outbound TCP connections with a bounded dial time.
type TCPDialer struct {
	Timeout time.Duration
}

// DialContext opens a TCP connection while respecting the caller's context.
func (d TCPDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	dialer := net.Dialer{Timeout: timeout}
	return dialer.DialContext(ctx, network, address)
}
