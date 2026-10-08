
package core

import (
	"io"
)

// Proxy هسته اصلی Proxy را مدیریت می‌کند.
type Proxy struct {
}

// NewProxy یک Proxy جدید ایجاد می‌کند.
func NewProxy() *Proxy {
	return &Proxy{}
}

// Handle یک Session را اجرا می‌کند.
//
// Session شامل Local و Remote Connection است.
// Proxy وظیفه دارد داده را در هر دو جهت بین این دو Connection
// منتقل کند.
func (p *Proxy) Handle(session *Session) error {
	if session == nil {
		return nil
	}

	session.Start()

	errCh := make(chan error, 2)

	// Local → Remote
	go func() {
		n, err := io.Copy(session.Remote, session.Local)

		if n > 0 {
			session.AddSentBytes(uint64(n))
		}

		errCh <- err
	}()

	// Remote → Local
	go func() {
		n, err := io.Copy(session.Local, session.Remote)

		if n > 0 {
			session.AddReceivedBytes(uint64(n))
		}

		errCh <- err
	}()

	// با پایان یکی از مسیرها، Session را می‌بندیم.
	err := <-errCh

	session.Close()

	return err
}
