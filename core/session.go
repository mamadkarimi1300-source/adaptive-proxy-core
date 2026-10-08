
package core

import (
	"sync"
	"time"

	"proxy/connection"
)

// SessionState وضعیت فعلی Session را مشخص می‌کند.
type SessionState string

const (
	SessionNew    SessionState = "new"
	SessionActive SessionState = "active"
	SessionClosed SessionState = "closed"
)

// Session یک ارتباط کامل Proxy را مدیریت می‌کند.
//
// هر Session شامل دو Connection است:
//   - Local: اتصال بین Client و Proxy
//   - Remote: اتصال بین Proxy و Server
type Session struct {
	ID string

	Local  connection.Connection
	Remote connection.Connection

	State SessionState

	CreatedAt time.Time
	ClosedAt  time.Time

	BytesSent     uint64
	BytesReceived uint64

	mu sync.RWMutex
}

// NewSession یک Session جدید ایجاد می‌کند.
func NewSession(
	id string,
	local connection.Connection,
	remote connection.Connection,
) *Session {
	return &Session{
		ID:        id,
		Local:     local,
		Remote:    remote,
		State:     SessionNew,
		CreatedAt: time.Now(),
	}
}

// Start Session را فعال می‌کند.
func (s *Session) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.State != SessionNew {
		return
	}

	s.State = SessionActive
}

// Close Session را می‌بندد و Connectionهای آن را نیز می‌بندد.
func (s *Session) Close() error {
	s.mu.Lock()

	if s.State == SessionClosed {
		s.mu.Unlock()
		return nil
	}

	s.State = SessionClosed
	s.ClosedAt = time.Now()

	local := s.Local
	remote := s.Remote

	s.mu.Unlock()

	var firstErr error

	if local != nil {
		if err := local.Close(); err != nil {
			firstErr = err
		}
	}

	if remote != nil {
		if err := remote.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// AddSentBytes تعداد Byteهای ارسال‌شده از Client به Server را ثبت می‌کند.
func (s *Session) AddSentBytes(n uint64) {
	s.mu.Lock()
	s.BytesSent += n
	s.mu.Unlock()
}

// AddReceivedBytes تعداد Byteهای دریافت‌شده از Server را ثبت می‌کند.
func (s *Session) AddReceivedBytes(n uint64) {
	s.mu.Lock()
	s.BytesReceived += n
	s.mu.Unlock()
}

// IsActive مشخص می‌کند Session هنوز فعال است یا خیر.
func (s *Session) IsActive() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.State == SessionActive
}
