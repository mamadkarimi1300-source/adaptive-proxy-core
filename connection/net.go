package connection

import "net"

// NetConnection adapts any net.Conn to the Connection interface.
type NetConnection struct {
	conn net.Conn
}

// NewNetConnection wraps a network connection.
func NewNetConnection(conn net.Conn) *NetConnection {
	return &NetConnection{conn: conn}
}

func (c *NetConnection) Read(p []byte) (int, error)  { return c.conn.Read(p) }
func (c *NetConnection) Write(p []byte) (int, error) { return c.conn.Write(p) }
func (c *NetConnection) Close() error                { return c.conn.Close() }

// CloseWrite half-closes the write side when the underlying connection supports it.
func (c *NetConnection) CloseWrite() error {
	if conn, ok := c.conn.(interface{ CloseWrite() error }); ok {
		return conn.CloseWrite()
	}
	return c.conn.Close()
}

// NetConn exposes the wrapped connection for protocol-specific operations.
func (c *NetConnection) NetConn() net.Conn { return c.conn }
