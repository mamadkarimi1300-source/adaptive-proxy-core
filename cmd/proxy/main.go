package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/adaptive"
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/config"
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/proxy"
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/transport"
)

func main() {
	logger := log.New(os.Stdout, "proxy: ", log.LstdFlags|log.Lmicroseconds)
	cfg := config.Load()
	selector, err := adaptive.NewSelector(transport.TCP{})
	if err != nil { logger.Fatal(err) }

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &proxy.Server{Address: cfg.ListenAddress, Selector: selector, Logger: logger}
	if err := server.ListenAndServe(ctx); err != nil { logger.Fatal(err) }
}
