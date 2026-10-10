package socks5

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"syscall"
	"time"

	"proxy/connection"
	"proxy/core"
	"proxy/transport"
)

const (
	version5       = 0x05
	methodNoAuth   = 0x00
	methodRejected = 0xff
	commandConnect = 0x01
	addressIPv4    = 0x01
	addressDomain  = 0x03
	addressIPv6    = 0x04
	replySucceeded = 0x00
	replyFailure   = 0x01
	replyNotAllowed = 0x02
	replyNetworkUnreachable = 0x03
	replyHostUnreachable = 0x04
	replyConnectionRefused = 0x05
	replyCommandUnsupported = 0x07
	replyAddressUnsupported = 0x08
)

// Server handles SOCKS5 CONNECT requests and delegates stream relay to core.Proxy.
// This initial implementation supports only the no-authentication method.
type Server struct {
	Dialer transport.Dialer
	Proxy  *core.Proxy
}

// NewServer creates a SOCKS5 server.
func NewServer(dialer transport.Dialer, proxy *core.Proxy) *Server {
	return &Server{Dialer: dialer, Proxy: proxy}
}

// ServeConn handles one accepted client connection.
func (s *Server) ServeConn(ctx context.Context, client net.Conn) error {
	if client == nil {
		return errors.New("nil client connection")
	}
	defer client.Close()
	if s.Dialer == nil || s.Proxy == nil {
		return errors.New("SOCKS5 server requires a dialer and proxy")
	}

	_ = client.SetDeadline(time.Now().Add(10 * time.Second))
	if err := negotiate(client); err != nil {
		return err
	}
	host, port, err := readConnectRequest(client)
	if err != nil {
		_ = writeReply(client, requestReplyCode(err))
		return err
	}
	_ = client.SetDeadline(time.Time{})

	target := net.JoinHostPort(host, strconv.Itoa(int(port)))
	upstream, err := s.Dialer.DialContext(ctx, "tcp", target)
	if err != nil {
		_ = writeReply(client, mapDialError(err))
		return fmt.Errorf("dial target %s: %w", target, err)
	}
	defer upstream.Close()

	if err := writeReply(client, replySucceeded); err != nil {
		return err
	}

	localConn := connection.NewNetConnection(client)
	remoteConn := connection.NewNetConnection(upstream)
	session := core.NewSession(client.RemoteAddr().String(), localConn, remoteConn)
	return s.Proxy.Handle(session)
}

func negotiate(rw io.ReadWriter) error {
	header := make([]byte, 2)
	if _, err := io.ReadFull(rw, header); err != nil {
		return err
	}
	if header[0] != version5 {
		return fmt.Errorf("unsupported SOCKS version %d", header[0])
	}
	methods := make([]byte, int(header[1]))
	if _, err := io.ReadFull(rw, methods); err != nil {
		return err
	}
	for _, method := range methods {
		if method == methodNoAuth {
			_, err := rw.Write([]byte{version5, methodNoAuth})
			return err
		}
	}
	_, _ = rw.Write([]byte{version5, methodRejected})
	return errors.New("client and server have no supported authentication method")
}

func readConnectRequest(r io.Reader) (string, uint16, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return "", 0, err
	}
	if header[0] != version5 {
		return "", 0, errors.New("invalid SOCKS version in request")
	}
	if header[1] != commandConnect {
		return "", 0, fmt.Errorf("unsupported SOCKS command %d", header[1])
	}
	if header[2] != 0 {
		return "", 0, errors.New("invalid reserved byte")
	}

	var host string
	switch header[3] {
	case addressIPv4:
		buf := make([]byte, net.IPv4len)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", 0, err
		}
		host = net.IP(buf).String()
	case addressIPv6:
		buf := make([]byte, net.IPv6len)
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", 0, err
		}
		host = net.IP(buf).String()
	case addressDomain:
		length := []byte{0}
		if _, err := io.ReadFull(r, length); err != nil {
			return "", 0, err
		}
		if length[0] == 0 {
			return "", 0, errors.New("empty destination domain")
		}
		buf := make([]byte, int(length[0]))
		if _, err := io.ReadFull(r, buf); err != nil {
			return "", 0, err
		}
		host = string(buf)
	default:
		return "", 0, fmt.Errorf("unsupported address type %d", header[3])
	}

	portBytes := make([]byte, 2)
	if _, err := io.ReadFull(r, portBytes); err != nil {
		return "", 0, err
	}
	return host, binary.BigEndian.Uint16(portBytes), nil
}

func writeReply(w io.Writer, code byte) error {
	// A wildcard IPv4 bind address and port zero are sufficient for CONNECT.
	payload := []byte{version5, code, 0x00, addressIPv4, 0, 0, 0, 0, 0, 0}
	for len(payload) > 0 {
		n, err := w.Write(payload)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		payload = payload[n:]
	}
	return nil
}

func requestReplyCode(err error) byte {
	switch {
	case strings.Contains(err.Error(), "unsupported SOCKS command"):
		return replyCommandUnsupported
	case strings.Contains(err.Error(), "unsupported address type"):
		return replyAddressUnsupported
	default:
		return replyFailure
	}
}

func mapDialError(err error) byte {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		if errors.Is(opErr.Err, syscallConnectionRefused()) {
			return replyConnectionRefused
		}
		if opErr.Op == "dial" {
			return replyHostUnreachable
		}
	}
	return replyFailure
}

func syscallConnectionRefused() error { return syscall.ECONNREFUSED }
