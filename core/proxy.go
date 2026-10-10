package core

import (
	"errors"
	"io"
	"sync"

	"proxy/connection"
)

// Proxy relays bytes between the two ends of a session.
type Proxy struct{}

func NewProxy() *Proxy { return &Proxy{} }

// Handle runs both copy directions. A clean EOF half-closes the opposite
// write side; a real copy error or completion of both directions ends the session.
func (p *Proxy) Handle(session *Session) error {
	if session == nil {
		return nil
	}
	if session.Local == nil || session.Remote == nil {
		_ = session.Close()
		return errors.New("proxy session requires local and remote connections")
	}

	session.Start()
	results := make(chan copyResult, 2)
	var once sync.Once

	copyDirection := func(dst, src connection.Connection, sent bool) {
		n, err := io.Copy(dst, src)
		if n > 0 {
			if sent {
				session.AddSentBytes(uint64(n))
			} else {
				session.AddReceivedBytes(uint64(n))
			}
		}

		// Preserve TCP half-close semantics when the source reaches EOF.
		if err == nil {
			if cw, ok := dst.(interface{ CloseWrite() error }); ok {
				err = cw.CloseWrite()
			}
		}
		results <- copyResult{err: err}
	}

	go copyDirection(session.Remote, session.Local, true)
	go copyDirection(session.Local, session.Remote, false)

	var firstErr error
	for i := 0; i < 2; i++ {
		result := <-results
		if result.err != nil && !errors.Is(result.err, io.EOF) && firstErr == nil {
			firstErr = result.err
			once.Do(func() { _ = session.Close() })
		}
	}
	_ = session.Close()
	return firstErr
}

type copyResult struct{ err error }
