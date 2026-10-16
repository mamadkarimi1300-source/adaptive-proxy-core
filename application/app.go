package application

import (
	"context"
	"errors"
	"log"
	"net"
	"sync"

	"proxy/config"
	"proxy/core"
	"proxy/protocol/socks5"
	"proxy/transport"
)

// App wires the proxy components and owns the listening socket.
type App struct {
	cfg    config.Config
	server *socks5.Server
}

// New creates the application and its in-process components.
func New(cfg config.Config) *App {
	tcp := transport.TCPDialer{Timeout: cfg.DialTimeout}
	candidates := []transport.Candidate{{Name: "tcp-direct", Dialer: tcp}}
	if cfg.QUICAddress != "" {
		candidates = append(candidates, transport.Candidate{
			Name: "quic-tunnel",
			Dialer: transport.QUICTunnelDialer{
				Address: cfg.QUICAddress, ServerName: cfg.QUICServerName,
				Token: cfg.QUICToken, Timeout: cfg.DialTimeout,
			},
		})
	}
	dialer, err := transport.NewAdaptiveDialer(candidates...)
	if err != nil {
		// The fixed candidate list above is validated by construction.
		log.Printf("adaptive dialer configuration error: %v", err)
		dialer = nil
	}
	server := socks5.NewServer(dialer, core.NewProxy())
	return &App{cfg: cfg, server: server}
}

// Run listens until the context is cancelled or the listener fails.
func (a *App) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", a.cfg.ListenAddr)
	if err != nil { return err }
	defer listener.Close()
	log.Printf("SOCKS5 proxy listening on %s", a.cfg.ListenAddr)
	if a.cfg.QUICAddress != "" {
		log.Printf("adaptive transport enabled: direct TCP and QUIC tunnel (%s)", a.cfg.QUICAddress)
	} else {
		log.Printf("adaptive transport enabled: direct TCP only; set PROXY_QUIC_ADDRESS to add QUIC")
	}

	var clients sync.WaitGroup
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done(): _ = listener.Close()
		case <-stop:
		}
	}()
	defer close(stop)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) { break }
			log.Printf("accept error: %v", err)
			continue
		}
		clients.Add(1)
		go func() {
			defer clients.Done()
			defer conn.Close()
			done := make(chan struct{})
			go func() {
				select {
				case <-ctx.Done(): _ = conn.Close()
				case <-done:
				}
			}()
			defer close(done)
			if err := a.server.ServeConn(ctx, conn); err != nil && ctx.Err() == nil {
				log.Printf("client %s: %v", conn.RemoteAddr(), err)
			}
		}()
	}
	clients.Wait()
	return nil
}
