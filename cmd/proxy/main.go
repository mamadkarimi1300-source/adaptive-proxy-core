package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"proxy/application"
	"proxy/config"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("configuration error: ", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	app := application.New(cfg)
	if err := app.Run(ctx); err != nil {
		log.Fatal("proxy stopped with error: ", err)
	}
}
