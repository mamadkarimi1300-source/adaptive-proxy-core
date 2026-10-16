package transport

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/quic-go/quic-go"
)

const quicTunnelALPN = "adaptive-proxy/1"

// QUICTunnelDialer opens a stream through a remote QUIC proxy, which then dials the target over TCP.
// ServerName must match the remote server certificate. Token must match the server's configured token.
type QUICTunnelDialer struct {
	Address    string
	ServerName string
	Token      string
	Timeout    time.Duration
	TLSConfig  *tls.Config
}

func (d QUICTunnelDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	if network != "tcp" {
		return nil, fmt.Errorf("QUIC tunnel only supports TCP streams, got %q", network)
	}
	if d.Address == "" || d.ServerName == "" || d.Token == "" {
		return nil, errors.New("QUIC tunnel requires address, server name, and token")
	}
	timeout := d.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	dialCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS13, ServerName: d.ServerName, NextProtos: []string{quicTunnelALPN}}
	if d.TLSConfig != nil {
		tlsConfig = d.TLSConfig.Clone()
		tlsConfig.MinVersion = tls.VersionTLS13
		tlsConfig.ServerName = d.ServerName
		tlsConfig.NextProtos = []string{quicTunnelALPN}
	}
	conn, err := quic.DialAddr(dialCtx, d.Address, tlsConfig, &quic.Config{HandshakeIdleTimeout: timeout, MaxIdleTimeout: 2 * time.Minute})
	if err != nil {
		return nil, fmt.Errorf("QUIC connect: %w", err)
	}
	stream, err := conn.OpenStreamSync(dialCtx)
	if err != nil {
		_ = conn.CloseWithError(0, "open stream failed")
		return nil, err
	}

	if err := writeTunnelRequest(stream, d.Token, address); err != nil {
		_ = conn.CloseWithError(0, "write request failed")
		return nil, err
	}
	var status [1]byte
	if _, err := io.ReadFull(stream, status[:]); err != nil {
		_ = conn.CloseWithError(0, "read response failed")
		return nil, err
	}
	if status[0] != 0 {
		_ = conn.CloseWithError(0, "remote target dial failed")
		return nil, errors.New("remote proxy could not connect to target")
	}
	return &quicStreamConn{Stream: stream, conn: conn}, nil
}

func writeTunnelRequest(w io.Writer, token, target string) error {
	if len(token) > 65535 || len(target) > 65535 {
		return errors.New("QUIC tunnel token or target is too long")
	}
	if err := binary.Write(w, binary.BigEndian, uint16(len(token))); err != nil { return err }
	if _, err := io.WriteString(w, token); err != nil { return err }
	if err := binary.Write(w, binary.BigEndian, uint16(len(target))); err != nil { return err }
	_, err := io.WriteString(w, target)
	return err
}

// ServeQUIC accepts authenticated tunnel streams and forwards each one to a TCP destination.
// The caller owns listener lifecycle and must supply a TLS certificate and a non-empty token.
func ServeQUIC(ctx context.Context, listener *quic.Listener, token string, dialTimeout time.Duration) error {
	if listener == nil || token == "" {
		return errors.New("QUIC server requires a listener and non-empty token")
	}
	if dialTimeout <= 0 { dialTimeout = 10 * time.Second }
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()
	for {
		session, err := listener.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil { return nil }
			return err
		}
		go handleQUICSession(ctx, session, token, dialTimeout)
	}
}

func handleQUICSession(ctx context.Context, session *quic.Conn, token string, dialTimeout time.Duration) {
	defer session.CloseWithError(0, "session ended")
	for {
		stream, err := session.AcceptStream(ctx)
		if err != nil { return }
		go handleQUICStream(ctx, stream, token, dialTimeout)
	}
}

func handleQUICStream(ctx context.Context, stream *quic.Stream, token string, dialTimeout time.Duration) {
	defer stream.Close()
	readString := func() (string, error) {
		var size uint16
		if err := binary.Read(stream, binary.BigEndian, &size); err != nil { return "", err }
		buf := make([]byte, int(size))
		_, err := io.ReadFull(stream, buf)
		return string(buf), err
	}
	gotToken, err := readString()
	if err != nil || gotToken != token {
		_, _ = stream.Write([]byte{1})
		stream.CancelRead(1)
		return
	}
	target, err := readString()
	if err != nil || target == "" {
		_, _ = stream.Write([]byte{1})
		return
	}
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	upstream, err := (&net.Dialer{Timeout: dialTimeout}).DialContext(dialCtx, "tcp", target)
	if err != nil {
		_, _ = stream.Write([]byte{1})
		return
	}
	defer upstream.Close()
	if _, err := stream.Write([]byte{0}); err != nil { return }

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(upstream, stream); if tcp, ok := upstream.(*net.TCPConn); ok { _ = tcp.CloseWrite() }; done <- struct{}{} }()
	go func() { _, _ = io.Copy(stream, upstream); _ = stream.Close(); done <- struct{}{} }()
	select {
	case <-ctx.Done():
	case <-done:
	}
}

type quicStreamConn struct {
	*quic.Stream
	conn *quic.Conn
}

func (c *quicStreamConn) LocalAddr() net.Addr { return c.conn.LocalAddr() }
func (c *quicStreamConn) RemoteAddr() net.Addr { return c.conn.RemoteAddr() }
func (c *quicStreamConn) Close() error {
	c.Stream.CancelRead(0)
	streamErr := c.Stream.Close()
	connErr := c.conn.CloseWithError(0, "stream closed")
	if streamErr != nil { return streamErr }
	return connErr
}
func (c *quicStreamConn) CloseWrite() error { return c.Stream.Close() }
