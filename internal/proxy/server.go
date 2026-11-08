package proxy

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/adaptive"
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/storage"
)

const (
	socksVersion = 5
	methodNoAuth = 0
	cmdConnect = 1
	atypIPv4 = 1
	atypDomain = 3
	atypIPv6 = 4
	repSucceeded = 0
	repGeneralFailure = 1
	repCommandUnsupported = 7
	repAddressUnsupported = 8
)

var sessionSequence atomic.Uint64

// Server accepts SOCKS5 clients and routes CONNECT requests through Selector.
type Server struct {
	Address  string
	Selector *adaptive.Selector
	Logger   *log.Logger
	Events   *storage.Logger
}

func (s *Server) ListenAndServe(ctx context.Context) error {
	if s.Selector == nil {
		return errors.New("adaptive selector is required")
	}
	ln, err := net.Listen("tcp", s.Address)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.Address, err)
	}
	defer ln.Close()
	var wg sync.WaitGroup
	var active sync.Map
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		active.Range(func(key, value any) bool {
			if conn, ok := value.(net.Conn); ok {
				_ = conn.Close()
			}
			return true
		})
	}()
	if s.Logger != nil {
		s.Logger.Printf("SOCKS5 listening on %s", ln.Addr())
	}
	defer wg.Wait()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return err
		}
		active.Store(conn, conn)
		wg.Add(1)
		go func(c net.Conn) {
			defer wg.Done()
			defer active.Delete(c)
			defer c.Close()
			s.handle(ctx, c)
		}(conn)
	}
}

func (s *Server) handle(parent context.Context, client net.Conn) {
	sessionID := fmt.Sprintf("session-%d", sessionSequence.Add(1))
	s.record(storage.Event{
		SessionID: sessionID,
		EventType: "connection_accepted",
		Network:   "tcp",
		Status:    "accepted",
	})

	_ = client.SetDeadline(time.Now().Add(15 * time.Second))
	target, err := readConnectRequest(client)
	if err != nil {
		s.record(storage.Event{
			SessionID:    sessionID,
			EventType:    "handshake_failed",
			Network:      "tcp",
			Status:       "failed",
			ErrorMessage: err.Error(),
		})
		if s.Logger != nil {
			s.Logger.Printf("SOCKS5 request rejected: %v", err)
		}
		return
	}
	_ = client.SetDeadline(time.Time{})
	s.record(storage.Event{
		SessionID:   sessionID,
		EventType:   "handshake_completed",
		Destination: target,
		Network:     "tcp",
		Status:      "success",
	})

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	dialStarted := time.Now()
	remote, route, err := s.Selector.Dial(ctx, "tcp", target)
	if err != nil {
		_ = writeReply(client, repGeneralFailure)
		s.record(storage.Event{
			SessionID:    sessionID,
			EventType:    "connection_failed",
			Destination:  target,
			Network:      "tcp",
			LatencyMS:    time.Since(dialStarted).Milliseconds(),
			Status:       "failed",
			ErrorMessage: err.Error(),
		})
		if s.Logger != nil {
			s.Logger.Printf("dial %s failed: %v", target, err)
		}
		return
	}
	defer remote.Close()
	if err := writeReply(client, repSucceeded); err != nil {
		return
	}
	s.record(storage.Event{
		SessionID:   sessionID,
		EventType:   "transport_selected",
		Destination: route.Destination,
		Transport:   route.Transport,
		Network:     "tcp",
		LatencyMS:   route.Latency.Milliseconds(),
		Status:      "success",
	})
	s.record(storage.Event{
		SessionID:   sessionID,
		EventType:   "connection_established",
		Destination: route.Destination,
		Transport:   route.Transport,
		Network:     "tcp",
		LatencyMS:   route.Latency.Milliseconds(),
		Status:      "success",
	})
	if s.Logger != nil {
		s.Logger.Printf("route destination=%s transport=%s dial=%s", route.Destination, route.Transport, route.Latency)
	}
	bytesSent, bytesReceived := relay(client, remote)
	s.record(storage.Event{
		SessionID:     sessionID,
		EventType:     "connection_closed",
		Destination:   route.Destination,
		Transport:     route.Transport,
		Network:       "tcp",
		BytesSent:     bytesSent,
		BytesReceived: bytesReceived,
		Status:        "closed",
	})
}

func (s *Server) record(event storage.Event) {
	if s.Events != nil {
		s.Events.Record(event)
	}
}

func readConnectRequest(conn net.Conn) (string, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", err
	}
	if header[0] != socksVersion {
		return "", errors.New("unsupported SOCKS version")
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(conn, methods); err != nil {
		return "", err
	}
	noAuth := false
	for _, m := range methods {
		if m == methodNoAuth {
			noAuth = true
			break
		}
	}
	if !noAuth {
		_, _ = conn.Write([]byte{socksVersion, 0xff})
		return "", errors.New("client offered no supported authentication method")
	}
	if _, err := conn.Write([]byte{socksVersion, methodNoAuth}); err != nil {
		return "", err
	}

	req := make([]byte, 4)
	if _, err := io.ReadFull(conn, req); err != nil {
		return "", err
	}
	if req[0] != socksVersion {
		return "", errors.New("invalid SOCKS request version")
	}
	if req[1] != cmdConnect {
		_ = writeReply(conn, repCommandUnsupported)
		return "", errors.New("only CONNECT is supported")
	}
	var host string
	switch req[3] {
	case atypIPv4:
		b := make([]byte, 4)
		if _, err := io.ReadFull(conn, b); err != nil {
			return "", err
		}
		host = net.IP(b).String()
	case atypIPv6:
		b := make([]byte, 16)
		if _, err := io.ReadFull(conn, b); err != nil {
			return "", err
		}
		host = net.IP(b).String()
	case atypDomain:
		n := []byte{0}
		if _, err := io.ReadFull(conn, n); err != nil {
			return "", err
		}
		b := make([]byte, int(n[0]))
		if _, err := io.ReadFull(conn, b); err != nil {
			return "", err
		}
		host = string(b)
	default:
		_ = writeReply(conn, repAddressUnsupported)
		return "", errors.New("unsupported address type")
	}
	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(conn, portBytes); err != nil {
		return "", err
	}
	port := binary.BigEndian.Uint16(portBytes)
	if host == "" || port == 0 {
		return "", errors.New("empty host or invalid port")
	}
	return net.JoinHostPort(host, strconv.Itoa(int(port))), nil
}

func writeReply(conn net.Conn, code byte) error {
	// The bound address is 0.0.0.0:0; this is valid for a CONNECT response.
	_, err := conn.Write([]byte{socksVersion, code, 0, atypIPv4, 0, 0, 0, 0, 0, 0})
	return err
}

type relayCounts struct {
	sent     int64
	received int64
}

func relay(client, remote net.Conn) (int64, int64) {
	done := make(chan relayCounts, 2)
	copyStream := func(dst, src net.Conn, clientToRemote bool) {
		n, _ := io.Copy(dst, src)
		if tcp, ok := dst.(*net.TCPConn); ok {
			_ = tcp.CloseWrite()
		}
		counts := relayCounts{}
		if clientToRemote {
			counts.sent = n
		} else {
			counts.received = n
		}
		done <- counts
	}
	go copyStream(remote, client, true)
	go copyStream(client, remote, false)
	first := <-done
	_ = client.Close()
	_ = remote.Close()
	second := <-done
	return first.sent + second.sent, first.received + second.received
}
