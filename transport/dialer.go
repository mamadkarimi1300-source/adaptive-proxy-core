package transport

import (
	"context"
	"net"
)

// Dialer creates outbound network connections for proxy requests.
type Dialer interface {
	DialContext(ctx context.Context, network, address string) (net.Conn, error)
}
