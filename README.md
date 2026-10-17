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
```

- **Inbound:** SOCKS5 CONNECT, no authentication.
- **Adaptive selector:** picks a transport on the first connection to a destination hostname and remembers it for later connections to that hostname.
- **Transport:** currently ships with TCP. Additional transports can implement the same interface.
- **Failure recovery:** if a remembered transport cannot connect, its affinity entry is cleared and the selector tries the available transports again.
- **Measurements:** records dial latency and success/failure counts per transport and destination.

The current build has only TCP registered, so it provides the adaptive selection framework and sticky destination routing, not a claim that TCP/QUIC/HTTP3 are already competing. A QUIC transport needs a compatible remote endpoint/tunnel protocol before it can carry arbitrary SOCKS5 traffic.

## Run

```sh
go run ./cmd/proxy
```

The default listen address is `127.0.0.1:1080`. Override it with `PROXY_LISTEN_ADDR`.

Configure a client to use SOCKS5 at `127.0.0.1:1080`. Use remote DNS mode (for example, `curl --socks5-hostname 127.0.0.1:1080 https://example.com`) to pass hostnames through the proxy.

## Scope

This is an educational foundation. It has no user authentication, access-control list, metrics endpoint, or encrypted upstream tunnel. Do not expose it publicly without adding access controls.
