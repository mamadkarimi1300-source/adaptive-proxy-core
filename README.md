# Adaptive Proxy Core

A small, extensible SOCKS5 proxy core in Go.

## Architecture

```text
Client
  -> SOCKS5 inbound
  -> Proxy session
  -> Adaptive route selector (sticky per destination hostname)
  -> Transport interface
  -> Destination

Proxy lifecycle events
  -> bounded asynchronous logger
  -> SQLite repository
  -> data/proxy.db
```

- **Inbound:** SOCKS5 CONNECT, no authentication.
- **Adaptive selector:** picks a transport on the first connection to a destination hostname and remembers it for later connections to that hostname.
- **Transport:** currently ships with TCP. Additional transports can implement the same interface.
- **Failure recovery:** if a remembered transport cannot connect, its affinity entry is cleared and the selector tries the available transports again.
- **Measurements:** records dial latency and success/failure counts per transport and destination.
- **Connection logs:** stores connection lifecycle events and final relay byte counts in SQLite. Log writes use a bounded asynchronous queue so the request handler does not wait for database I/O.

The current build has only TCP registered, so it provides the adaptive selection framework and sticky destination routing, not a claim that TCP/QUIC/HTTP3 are already competing. A QUIC transport needs a compatible remote endpoint/tunnel protocol before it can carry arbitrary SOCKS5 traffic.

## Run

On a fresh checkout, resolve dependencies and generate the local checksum file once:

```sh
go mod tidy
go run ./cmd/proxy
```

The default listen address is `127.0.0.1:1080`. Override it with `PROXY_LISTEN_ADDR`.

The SQLite database is created automatically at `data/proxy.db`. Override its path with `PROXY_LOG_DB_PATH`. The parent directory is created automatically.

Example:

```sh
PROXY_LOG_DB_PATH=./data/proxy.db go run ./cmd/proxy
```

Configure a client to use SOCKS5 at `127.0.0.1:1080`. Use remote DNS mode (for example, `curl --socks5-hostname 127.0.0.1:1080 https://example.com`) to pass hostnames through the proxy.

## Connection log events

The `connection_logs` table records:

- `connection_accepted`
- `handshake_completed` / `handshake_failed`
- `transport_selected`
- `connection_established` / `connection_failed`
- `connection_closed`

Each event includes a session ID, timestamp, destination (when known), transport, network, dial latency, status, and error details. The close event also records bytes sent from client to destination and bytes received from destination to client.

The logger uses a bounded in-memory queue (default 1024 events). If the queue is full, new log events are dropped rather than blocking proxy traffic. The logger drains queued events during graceful shutdown. Database write errors are currently not surfaced to the proxy logger; operational error reporting and retention/rotation can be added separately.

## Scope

This is an educational foundation. It has no user authentication, access-control list, metrics endpoint, or encrypted upstream tunnel. Do not expose it publicly without adding access controls.
