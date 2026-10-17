package config

import (
	"os"
	"strings"
)

const defaultListenAddress = "127.0.0.1:1080"

// Config contains process-level settings.
type Config struct {
	ListenAddress string
}

// Load reads configuration from environment variables.
func Load() Config {
	addr := strings.TrimSpace(os.Getenv("PROXY_LISTEN_ADDR"))
	if addr == "" {
		addr = defaultListenAddress
	}
	return Config{ListenAddress: addr}
}
