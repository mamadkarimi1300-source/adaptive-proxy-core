package config

import (
	"errors"
	"os"
	"strings"
	"time"
)

// Config contains runtime settings for the local application proxy.
type Config struct {
	ListenAddr  string
	DialTimeout time.Duration
}

// Load reads configuration from environment variables and applies safe defaults.
func Load() (Config, error) {
	cfg := Config{
		ListenAddr:  "127.0.0.1:1080",
		DialTimeout: 10 * time.Second,
	}

	if value := strings.TrimSpace(os.Getenv("PROXY_LISTEN_ADDR")); value != "" {
		cfg.ListenAddr = value
	}
	if cfg.ListenAddr == "" {
		return Config{}, errors.New("proxy listen address must not be empty")
	}

	if value := strings.TrimSpace(os.Getenv("PROXY_DIAL_TIMEOUT")); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return Config{}, errors.New("PROXY_DIAL_TIMEOUT must be a positive duration, for example 10s")
		}
		cfg.DialTimeout = timeout
	}

	return cfg, nil
}
