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
	dialer := transport.TCPDialer{Timeout: cfg.DialTimeout}
	server := socks5.NewServer(dialer, core.NewProxy())
	return &App{cfg: cfg, server: server}
}

// Run listens until the context is cancelled or the listener fails.
func (a *App) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", a.cfg.ListenAddr)
	if err != nil {
		return err
	}
	defer listener.Close()

	log.Printf("SOCKS5 proxy listening on %s", a.cfg.ListenAddr)

	var clients sync.WaitGroup
	stop := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = listener.Close()
		case <-stop:
		}
	}()
	defer close(stop)

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				break
			}
			log.Printf("accept error: %v", err)
			continue
		}

		clients.Add(1)
		go func() {
			defer clients.Done()
			defer conn.Close()
			if err := a.server.ServeConn(ctx, conn); err != nil && ctx.Err() == nil {
				log.Printf("client %s: %v", conn.RemoteAddr(), err)
			}
		}()
	}
	clients.Wait()
	return nil
}
