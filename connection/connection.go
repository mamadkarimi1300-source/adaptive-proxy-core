package connection

import "io"

// Connection defines the common behavior of a proxy connection.
type Connection interface {
	io.Reader
	io.Writer
	io.Closer
}