package socks5

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
)

func TestReadConnectRequestDomain(t *testing.T) {
	input := []byte{version5, commandConnect, 0, addressDomain, 11}
	input = append(input, []byte("example.com")...)
	input = append(input, 0x01, 0xbb)

	host, port, err := readConnectRequest(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("readConnectRequest() error = %v", err)
	}
	if host != "example.com" {
		t.Fatalf("host = %q, want example.com", host)
	}
	if port != 443 {
		t.Fatalf("port = %d, want 443", port)
	}
}

func TestReadConnectRequestIPv4(t *testing.T) {
	input := []byte{version5, commandConnect, 0, addressIPv4, 192, 0, 2, 10, 0x01, 0xbb}
	host, port, err := readConnectRequest(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("readConnectRequest() error = %v", err)
	}
	if host != "192.0.2.10" || port != 443 {
		t.Fatalf("destination = %s:%d, want 192.0.2.10:443", host, port)
	}
}

func TestReadConnectRequestIPv6(t *testing.T) {
	ip := net.ParseIP("2001:db8::1").To16()
	input := []byte{version5, commandConnect, 0, addressIPv6}
	input = append(input, ip...)
	input = append(input, 0x00, 0x50)

	host, port, err := readConnectRequest(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("readConnectRequest() error = %v", err)
	}
	if net.ParseIP(host) == nil || port != 80 {
		t.Fatalf("destination = %s:%d, want IPv6 address and port 80", host, port)
	}
}

func TestReadConnectRequestRejectsUnsupportedCommand(t *testing.T) {
	input := []byte{version5, 0x02, 0, addressIPv4, 127, 0, 0, 1, 0, 80}
	_, _, err := readConnectRequest(bytes.NewReader(input))
	if err == nil {
		t.Fatal("expected unsupported command error")
	}
}

func TestWriteReply(t *testing.T) {
	var output bytes.Buffer
	if err := writeReply(&output, replySucceeded); err != nil {
		t.Fatalf("writeReply() error = %v", err)
	}
	got := output.Bytes()
	if len(got) != 10 || got[0] != version5 || got[1] != replySucceeded ||
		binary.BigEndian.Uint16(got[8:10]) != 0 {
		t.Fatalf("unexpected SOCKS5 reply: %v", got)
	}
}
