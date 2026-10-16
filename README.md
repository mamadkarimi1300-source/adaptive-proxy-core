# Adaptive Proxy Core

A small Go application-level proxy core. The local entry point is SOCKS5; outbound TCP connections can be made directly or through an authenticated QUIC tunnel to a remote proxy.

## Current scope

- SOCKS5 no-authentication negotiation and TCP `CONNECT` for IPv4, IPv6, and domain names.
- Direct outbound TCP and optional QUIC-over-UDP tunnel transport.
- Adaptive path selection based on observed connection setup latency (EWMA), with fallback when a selected path fails.
- QUIC tunnel authentication using a shared token protected by TLS 1.3.
- Session byte counters, connection lifecycle tracking, and graceful local listener shutdown.

Not implemented: SOCKS5 UDP ASSOCIATE, username/password authentication, active bandwidth/throughput probing, TUN, or OS-level transparent interception.

## Run the local proxy

Requires Go 1.23 or later.

```sh
go test ./...
go vet ./...
go run ./cmd/proxy
```

The default SOCKS5 listener is `127.0.0.1:1080`. It has no client authentication, so keep it bound to loopback unless you add suitable access controls.

### Enable the QUIC candidate

Configure the local proxy with:

- `PROXY_QUIC_ADDRESS`: remote server address, e.g. `proxy.example.com:4433`.
- `PROXY_QUIC_SERVER_NAME`: DNS name present in the server TLS certificate.
- `PROXY_QUIC_TOKEN`: shared secret; use a long random value.
- `PROXY_DIAL_TIMEOUT`: optional timeout (default `10s`).

Example (PowerShell):

```powershell
$env:PROXY_QUIC_ADDRESS = "proxy.example.com:4433"
$env:PROXY_QUIC_SERVER_NAME = "proxy.example.com"
$env:PROXY_QUIC_TOKEN = "replace-with-a-long-random-secret"
go run ./cmd/proxy
```

When configured, the selector explores both direct TCP and QUIC-tunnel paths, measures real connection setup duration, and prefers the path with the lower smoothed latency. Failed paths are penalized and the other candidate is attempted. This is not a download-speed benchmark: throughput measurement and periodic active probes are future work. The initial implementation creates a QUIC session for each tunneled connection; connection pooling can be added after correctness and measurements are established.

## Run the remote QUIC proxy server

The remote server accepts QUIC over UDP, authenticates each stream using the shared token, then dials the requested destination over TCP. Use a valid TLS certificate for the server name; clients verify the certificate normally.

```sh
export PROXY_QUIC_CERT_FILE=/etc/letsencrypt/live/proxy.example.com/fullchain.pem
export PROXY_QUIC_KEY_FILE=/etc/letsencrypt/live/proxy.example.com/privkey.pem
export PROXY_QUIC_TOKEN='replace-with-the-same-long-random-secret'
export PROXY_QUIC_LISTEN_ADDR=':4433'
go run ./cmd/quic-server
```

Allow UDP traffic to the configured port in the server firewall. Do not run the remote server without a strong token: it can connect to TCP destinations on behalf of authenticated clients. Keep the token secret and rotate it if exposed.

## Configuration

Local process:

- `PROXY_LISTEN_ADDR`: SOCKS5 listen address (default `127.0.0.1:1080`).
- `PROXY_DIAL_TIMEOUT`: outbound dial timeout (default `10s`).
- `PROXY_QUIC_ADDRESS`, `PROXY_QUIC_SERVER_NAME`, `PROXY_QUIC_TOKEN`: enable the QUIC candidate; all three are required together.

Remote process:

- `PROXY_QUIC_LISTEN_ADDR`: UDP listener (default `:4433`).
- `PROXY_QUIC_CERT_FILE`, `PROXY_QUIC_KEY_FILE`, `PROXY_QUIC_TOKEN`: required TLS credentials and shared token.

## Architecture

- `cmd/proxy`: local process entry point.
- `cmd/quic-server`: remote QUIC tunnel server.
- `application`: wires local components and owns the SOCKS5 listener.
- `config`: local runtime settings.
- `protocol/socks5`: SOCKS5 negotiation and CONNECT request parsing.
- `transport`: TCP dialer, QUIC tunnel, and adaptive candidate selection.
- `core`: session lifecycle and bidirectional relay.
- `connection`: connection adapters.

These are Go packages in one process, not separate microservices. SOCKS5 is the application-facing protocol; QUIC is an optional transport between the local and remote proxies; the remote proxy connects to final destinations over TCP.
