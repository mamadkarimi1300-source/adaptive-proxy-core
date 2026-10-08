package connection

import "net"

// TCPConnection represents a TCP-based connection.
type TCPConnection struct {
	conn *net.TCPConn
}

// NewTCPConnection creates a new TCPConnection.
func NewTCPConnection(conn *net.TCPConn) *TCPConnection {
	return &TCPConnection{
		conn: conn,
	}
}

// Read reads data from the TCP connection.
func (c *TCPConnection) Read(p []byte) (int, error) {
	return c.conn.Read(p)
}

// Write writes data to the TCP connection.
func (c *TCPConnection) Write(p []byte) (int, error) {
	return c.conn.Write(p)
}

// Close closes the TCP connection.
func (c *TCPConnection) Close() error {
	return c.conn.Close()
}