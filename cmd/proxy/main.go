package main

import (
	"log"
	"net"

	"proxy/connection"
	"proxy/core"
)

const (
	listenAddr = ":8080"
	remoteAddr = "127.0.0.1:9000"
)

func main() {
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	defer listener.Close()

	log.Println("proxy listening on", listenAddr)
	log.Println("proxy target:", remoteAddr)

	proxy := core.NewProxy()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}

		go handleClient(conn, proxy)
	}
}

func handleClient(conn net.Conn, proxy *core.Proxy) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		log.Println("unsupported connection type:", conn.RemoteAddr())
		conn.Close()
		return
	}

	local := connection.NewTCPConnection(tcpConn)

	log.Println("new client:", conn.RemoteAddr())

	remoteTCP, err := net.DialTCP("tcp", nil, mustResolveTCPAddr(remoteAddr))
	if err != nil {
		log.Println("remote dial error:", err)
		local.Close()
		return
	}

	remote := connection.NewTCPConnection(remoteTCP)

	session := core.NewSession(
		conn.RemoteAddr().String(),
		local,
		remote,
	)

	if err := proxy.Handle(session); err != nil {
		log.Println("proxy session error:", err)
	}

	log.Println(
		"session closed:",
		session.ID,
		"sent:",
		session.BytesSent,
		"received:",
		session.BytesReceived,
	)
}

func mustResolveTCPAddr(address string) *net.TCPAddr {
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		log.Fatal("resolve remote address error:", err)
	}

	return addr
}
