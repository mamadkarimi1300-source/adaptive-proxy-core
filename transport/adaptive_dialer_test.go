package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type testDialer struct {
	delay time.Duration
	err   error
	calls int
}

func (d *testDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.calls++
	if d.delay > 0 {
		timer := time.NewTimer(d.delay)
		defer timer.Stop()
		select {
		case <-ctx.Done(): return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if d.err != nil { return nil, d.err }
	left, right := net.Pipe()
	_ = right.Close()
	return left, nil
}

func TestAdaptiveDialerExploresCandidates(t *testing.T) {
	fast := &testDialer{delay: time.Millisecond}
	slow := &testDialer{delay: 15 * time.Millisecond}
	dialer, err := NewAdaptiveDialer(Candidate{Name: "tcp", Dialer: fast}, Candidate{Name: "quic", Dialer: slow})
	if err != nil { t.Fatal(err) }

	for i := 0; i < 2; i++ {
		conn, err := dialer.DialContext(context.Background(), "tcp", "example.com:443")
		if err != nil { t.Fatal(err) }
		_ = conn.Close()
	}
	if fast.calls != 1 || slow.calls != 1 {
		t.Fatalf("expected each candidate to be explored once; tcp=%d quic=%d", fast.calls, slow.calls)
	}
	measurements := dialer.Snapshot()
	if measurements["tcp"] >= measurements["quic"] {
		t.Fatalf("expected TCP measured faster than QUIC: %#v", measurements)
	}
}

func TestAdaptiveDialerFallsBackAfterFailure(t *testing.T) {
	broken := &testDialer{err: errors.New("path unavailable")}
	working := &testDialer{}
	dialer, err := NewAdaptiveDialer(Candidate{Name: "quic", Dialer: broken}, Candidate{Name: "tcp", Dialer: working})
	if err != nil { t.Fatal(err) }
	conn, err := dialer.DialContext(context.Background(), "tcp", "example.com:443")
	if err != nil { t.Fatalf("expected fallback to succeed: %v", err) }
	_ = conn.Close()
	if broken.calls != 1 || working.calls != 1 {
		t.Fatalf("expected failed path and fallback to be tried; quic=%d tcp=%d", broken.calls, working.calls)
	}
}

func TestNewAdaptiveDialerRejectsDuplicateNames(t *testing.T) {
	d := &testDialer{}
	if _, err := NewAdaptiveDialer(Candidate{Name: "same", Dialer: d}, Candidate{Name: "same", Dialer: d}); err == nil {
		t.Fatal("expected duplicate candidate names to be rejected")
	}
}
