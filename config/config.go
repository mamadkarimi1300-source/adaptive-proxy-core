package config

import (
	"errors"
	"os"
	"strings"
	"time"
)

// Config contains runtime settings for the local application proxy.
type Config struct {
	ListenAddr     string
	DialTimeout    time.Duration
	QUICAddress    string
	QUICServerName string
	QUICToken      string
}

// Load reads configuration from environment variables and applies safe defaults.
func Load() (Config, error) {
	cfg := Config{
		ListenAddr:  "127.0.0.1:1080",
		DialTimeout: 10 * time.Second,
	}
	read := func(key string) string { return strings.TrimSpace(os.Getenv(key)) }
	if value := read("PROXY_LISTEN_ADDR"); value != "" { cfg.ListenAddr = value }
	if cfg.ListenAddr == "" { return Config{}, errors.New("proxy listen address must not be empty") }
	if value := read("PROXY_DIAL_TIMEOUT"); value != "" {
		timeout, err := time.ParseDuration(value)
		if err != nil || timeout <= 0 {
			return Config{}, errors.New("PROXY_DIAL_TIMEOUT must be a positive duration, for example 10s")
		}
		cfg.DialTimeout = timeout
	}

	cfg.QUICAddress = read("PROXY_QUIC_ADDRESS")
	cfg.QUICServerName = read("PROXY_QUIC_SERVER_NAME")
	cfg.QUICToken = read("PROXY_QUIC_TOKEN")
	if cfg.QUICAddress != "" && (cfg.QUICServerName == "" || cfg.QUICToken == "") {
		return Config{}, errors.New("PROXY_QUIC_ADDRESS requires PROXY_QUIC_SERVER_NAME and PROXY_QUIC_TOKEN")
	}
	if cfg.QUICAddress == "" && (cfg.QUICServerName != "" || cfg.QUICToken != "") {
		return Config{}, errors.New("PROXY_QUIC_SERVER_NAME and PROXY_QUIC_TOKEN require PROXY_QUIC_ADDRESS")
	}
	return cfg, nil
}
