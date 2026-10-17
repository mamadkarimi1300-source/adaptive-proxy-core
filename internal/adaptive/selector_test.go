package adaptive

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

type fakeDialer struct {
	name string
	mu sync.Mutex
	calls int
	fail bool
	delay time.Duration
}

func (f *fakeDialer) Name() string { return f.name }
func (f *fakeDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	f.mu.Lock()
	f.calls++
	fail := f.fail
	delay := f.delay
	f.mu.Unlock()
	if delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done(): return nil, ctx.Err()
		case <-timer.C:
		}
	}
	if fail { return nil, errors.New("simulated dial failure") }
	a, b := net.Pipe()
	go func() { defer b.Close(); buf := make([]byte, 1); _, _ = b.Read(buf) }()
	return a, nil
}
func (f *fakeDialer) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestSelectorKeepsTransportForSameDestination(t *testing.T) {
	fast := &fakeDialer{name:"fast"}
	slow := &fakeDialer{name:"slow", delay:time.Millisecond}
	s, err := NewSelector(fast, slow)
	if err != nil { t.Fatal(err) }
	for i:=0; i<2; i++ {
		conn, route, err := s.Dial(context.Background(), "tcp", "example.com:443")
		if err != nil { t.Fatal(err) }
		_ = conn.Close()
		if route.Transport != "fast" { t.Fatalf("route transport = %q, want fast", route.Transport) }
	}
	if fast.callCount()!=2 { t.Fatalf("fast called %d times, want 2", fast.callCount()) }
	if slow.callCount()!=0 { t.Fatalf("slow called %d times, want 0", slow.callCount()) }
}

func TestSelectorUsesSeparateAffinityForDifferentDestinations(t *testing.T) {
	a := &fakeDialer{name:"a"}
	b := &fakeDialer{name:"b"}
	s, err := NewSelector(a,b)
	if err != nil { t.Fatal(err) }
	for _, target := range []string{"one.example:443","two.example:443"} {
		conn, _, err := s.Dial(context.Background(),"tcp",target)
		if err != nil { t.Fatal(err) }
		_ = conn.Close()
	}
	if a.callCount()+b.callCount()!=2 { t.Fatalf("expected one dial per new destination; got %d",a.callCount()+b.callCount()) }
}

func TestSelectorRetriesWhenRememberedTransportFails(t *testing.T) {
	first := &fakeDialer{name:"a"}
	second := &fakeDialer{name:"b"}
	s, err := NewSelector(first,second)
	if err != nil { t.Fatal(err) }
	conn, _, err := s.Dial(context.Background(),"tcp","example.com:443")
	if err != nil { t.Fatal(err) }
	_ = conn.Close()
	first.mu.Lock(); first.fail=true; first.mu.Unlock()
	conn, route, err := s.Dial(context.Background(),"tcp","example.com:443")
	if err != nil { t.Fatal(err) }
	_ = conn.Close()
	if route.Transport!="b" { t.Fatalf("transport after failure = %q, want b",route.Transport) }
	if got:=s.Observations(); len(got)!=2 { t.Fatalf("observations count = %d, want 2",len(got)) }
}
