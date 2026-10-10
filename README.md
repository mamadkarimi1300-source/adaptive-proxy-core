# Adaptive Proxy Core

A small Go application-level proxy core. The current implementation provides a local SOCKS5 server for TCP `CONNECT` requests and a bidirectional stream relay.

## Current scope

- SOCKS5 no-authentication negotiation.
- TCP `CONNECT` for IPv4, IPv6, and domain-name destinations.
- Outbound TCP dialing with a configurable timeout.
- Session byte counters and connection lifecycle tracking.
- Graceful listener shutdown and cancellation of active client connections.

Not implemented yet: SOCKS5 UDP ASSOCIATE, username/password authentication, adaptive route selection, active network measurement, TUN, or OS-level transparent interception.

## Run

Requires Go 1.23 or later.

```sh
go test ./...
go run ./cmd/proxy
```

The default listener is `127.0.0.1:1080`, intentionally bound to loopback. This version has no authentication, so do not expose it to an untrusted network.

Configuration:

- `PROXY_LISTEN_ADDR`: listener address (default `127.0.0.1:1080`).
- `PROXY_DIAL_TIMEOUT`: outbound dial timeout (default `10s`, Go duration syntax).

## Architecture

- `cmd/proxy`: process entry point and signal handling.
- `application`: wires components and owns the listener.
- `config`: runtime configuration.
- `protocol/socks5`: SOCKS5 negotiation and CONNECT request parsing.
- `transport`: outbound connection abstraction and TCP dialer.
- `core`: session lifecycle and bidirectional relay.
- `connection`: connection adapters.

These are Go packages in one process, not separate microservices. Keeping the protocol, transport, and relay boundaries separate makes later routing and measurement features easier to add without turning every component into a network service.
