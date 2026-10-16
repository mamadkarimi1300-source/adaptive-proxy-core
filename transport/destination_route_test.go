package transport

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAdaptiveDialerKeepsRoutePerDestination(t *testing.T) {
	tcp := &testDialer{}
	quic := &testDialer{}
	dialer, err := NewAdaptiveDialer(
		Candidate{Name: "tcp", Dialer: tcp},
		Candidate{Name: "quic", Dialer: quic},
	)
	if err != nil {
		t.Fatal(err)
	}

	dialAndClose := func(address string) {
		t.Helper()
		conn, err := dialer.DialContext(context.Background(), "tcp", address)
		if err != nil {
			t.Fatalf("dial %s: %v", address, err)
		}
		_ = conn.Close()
	}

	dialAndClose("google.com:443")
	if tcp.calls != 1 || quic.calls != 0 {
		t.Fatalf("first destination should explore tcp first; tcp=%d quic=%d", tcp.calls, quic.calls)
	}

	// Exploration order rotates globally, but the cached route for Google is sticky.
	dialAndClose("google.com:443")
	if tcp.calls != 2 || quic.calls != 0 {
		t.Fatalf("same destination should reuse tcp route; tcp=%d quic=%d", tcp.calls, quic.calls)
	}

	// A new destination has its own decision and can explore another candidate.
	dialAndClose("example.org:443")
	if tcp.calls != 2 || quic.calls != 1 {
		t.Fatalf("new destination should be evaluated independently; tcp=%d quic=%d", tcp.calls, quic.calls)
	}
}

func TestAdaptiveDialerInvalidatesFailedCachedRoute(t *testing.T) {
	tcp := &testDialer{}
	quic := &testDialer{}
	dialer, err := NewAdaptiveDialer(
		Candidate{Name: "tcp", Dialer: tcp},
		Candidate{Name: "quic", Dialer: quic},
	)
	if err != nil {
		t.Fatal(err)
	}

	conn, err := dialer.DialContext(context.Background(), "tcp", "google.com:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	tcp.err = errors.New("tcp path unavailable")
	conn, err = dialer.DialContext(context.Background(), "tcp", "google.com:443")
	if err != nil {
		t.Fatalf("expected fallback after cached route failure: %v", err)
	}
	_ = conn.Close()

	if tcp.calls != 2 || quic.calls != 1 {
		t.Fatalf("expected cached tcp failure and quic fallback; tcp=%d quic=%d", tcp.calls, quic.calls)
	}

	conn, err = dialer.DialContext(context.Background(), "tcp", "google.com:443")
	if err != nil {
		t.Fatalf("expected successful cached fallback route: %v", err)
	}
	_ = conn.Close()
	if quic.calls != 2 {
		t.Fatalf("expected quic route to be cached after fallback; quic=%d", quic.calls)
	}
}

func TestDestinationRouteExpires(t *testing.T) {
	tcp := &testDialer{}
	quic := &testDialer{}
	dialer, err := NewAdaptiveDialer(
		Candidate{Name: "tcp", Dialer: tcp},
		Candidate{Name: "quic", Dialer: quic},
	)
	if err != nil {
		t.Fatal(err)
	}
	dialer.routeTTL = time.Millisecond

	conn, err := dialer.DialContext(context.Background(), "tcp", "google.com:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	time.Sleep(5 * time.Millisecond)

	conn, err = dialer.DialContext(context.Background(), "tcp", "google.com:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	if tcp.calls+quic.calls != 2 {
		t.Fatalf("expected a fresh route decision after expiry; tcp=%d quic=%d", tcp.calls, quic.calls)
	}
}

func TestDestinationKeyNormalizesHost(t *testing.T) {
	if got, want := destinationKey("tcp", "GOOGLE.com.:443"), destinationKey("tcp", "google.com:443"); got != want {
		t.Fatalf("expected normalized destination keys to match: %q != %q", got, want)
	}
	if destinationKey("tcp", "google.com:443") == destinationKey("tcp", "google.com:80") {
		t.Fatal("different ports must have independent route decisions")
	}
}

// Ensure the fake dialer used by these tests satisfies the production interface.
var _ Dialer = (*testDialer)(nil)
