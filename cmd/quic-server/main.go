package main

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/quic-go/quic-go"
	"proxy/transport"
)

func main() {
	addr := env("PROXY_QUIC_LISTEN_ADDR", ":4433")
	certFile := env("PROXY_QUIC_CERT_FILE", "")
	keyFile := env("PROXY_QUIC_KEY_FILE", "")
	token := env("PROXY_QUIC_TOKEN", "")
	if certFile == "" || keyFile == "" || token == "" {
		log.Fatal("PROXY_QUIC_CERT_FILE, PROXY_QUIC_KEY_FILE, and PROXY_QUIC_TOKEN are required")
	}
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil { log.Fatal("load QUIC TLS certificate: ", err) }
	listener, err := quic.ListenAddr(addr, &tls.Config{
		Certificates: []tls.Certificate{certificate},
		MinVersion: tls.VersionTLS13,
		NextProtos: []string{"adaptive-proxy/1"},
	}, &quic.Config{HandshakeIdleTimeout: 10 * time.Second, MaxIdleTimeout: 2 * time.Minute})
	if err != nil { log.Fatal("listen for QUIC: ", err) }

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Printf("authenticated QUIC tunnel listening on UDP %s", addr)
	if err := transport.ServeQUIC(ctx, listener, token, 10*time.Second); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal("QUIC tunnel stopped: ", err)
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" { return value }
	return fallback
}
