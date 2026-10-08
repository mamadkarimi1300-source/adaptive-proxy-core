package main

import (
	"log"
	"net"

	"proxy/connection"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()

	log.Println("proxy listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}

		tcpConn, ok := conn.(*net.TCPConn)
		if !ok {
			conn.Close()
			continue
		}

		client := connection.NewTCPConnection(tcpConn)

		log.Println("new client:", conn.RemoteAddr())

		_ = client
	}
}