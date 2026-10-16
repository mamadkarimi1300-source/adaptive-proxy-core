package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

type testDialer struct {
	err error
	calls int
}

func (d *testDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	d.calls++
	if d.err != nil { return nil, d.err }
	left, right := net.Pipe()
	_ = right.Close()
	return left, nil
}

func TestAdaptiveDialerExploresCandidates(t *testing.T) {
	fast := &testDialer{}
	slow := &testDialer{}
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
	if len(dialer.Snapshot()) != 0 {
		t.Fatal("empty/short sessions must not be presented as valid throughput samples")
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

func TestThroughputEWMAAndSampleThreshold(t *testing.T) {
	dialer, err := NewAdaptiveDialer(Candidate{Name: "tcp", Dialer: &testDialer{}})
	if err != nil { t.Fatal(err) }
	dialer.recordThroughput("tcp", minThroughputSampleBytes-1, time.Second)
	if len(dialer.Snapshot()) != 0 {
		t.Fatal("sample below minimum byte threshold must be ignored")
	}
	dialer.recordThroughput("tcp", 1_000_000, time.Second)
	got := dialer.Snapshot()["tcp"]
	if got < 7.9 || got > 8.1 {
		t.Fatalf("expected about 8 Mbps, got %.3f Mbps", got)
	}
	dialer.recordThroughput("tcp", 2_000_000, time.Second)
	got = dialer.Snapshot()["tcp"]
	if got < 9.9 || got > 10.1 {
		t.Fatalf("expected EWMA about 10 Mbps, got %.3f Mbps", got)
	}
}

func TestNewAdaptiveDialerRejectsDuplicateNames(t *testing.T) {
	d := &testDialer{}
	if _, err := NewAdaptiveDialer(Candidate{Name: "same", Dialer: d}, Candidate{Name: "same", Dialer: d}); err == nil {
		t.Fatal("expected duplicate candidate names to be rejected")
	}
}
