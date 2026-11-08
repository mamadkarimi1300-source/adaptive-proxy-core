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
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/storage"
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/storage/sqlite"
	"github.com/mamadkarimi1300-source/adaptive-proxy-core/internal/transport"
)

func main() {
	logger := log.New(os.Stdout, "proxy: ", log.LstdFlags|log.Lmicroseconds)
	cfg := config.Load()

	store, err := sqlite.Open(cfg.LogDBPath)
	if err != nil {
		logger.Printf("initialize adaptive selector: %v", err)
		return
	}
	eventLogger := storage.NewLogger(store, 1024)
	defer func() {
		if err := eventLogger.Close(); err != nil {
			logger.Printf("close log database: %v", err)
		}
	}()

	selector, err := adaptive.NewSelector(transport.TCP{})
	if err != nil {
		logger.Printf("initialize adaptive selector: %v", err)
		return
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &proxy.Server{
		Address:  cfg.ListenAddress,
		Selector: selector,
		Logger:   logger,
		Events:   eventLogger,
	}
	if err := server.ListenAndServe(ctx); err != nil {
		logger.Printf("proxy server stopped: %v", err)
	}
}
