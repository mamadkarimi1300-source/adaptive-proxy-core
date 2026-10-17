package proxy

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
)

func TestReadConnectRequestDomain(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	result := make(chan struct {
		target string
		err error
	},1)
	go func() {
		target, err := readConnectRequest(server)
		result <- struct { target string; err error }{target,err}
	}()
	_, _ = client.Write([]byte{5,1,0})
	methodReply:=make([]byte,2)
	if _,err:=io.ReadFull(client,methodReply); err!=nil { t.Fatal(err) }
	if methodReply[0]!=5 || methodReply[1]!=0 { t.Fatalf("unexpected method reply: %v",methodReply) }
	host:=[]byte("example.com")
	request:=[]byte{5,cmdConnect,0,atypDomain,byte(len(host))}
	request=append(request,host...)
	port:=make([]byte,2)
	binary.BigEndian.PutUint16(port,443)
	request=append(request,port...)
	_,_=client.Write(request)
	got:=<-result
	if got.err!=nil { t.Fatal(got.err) }
	if got.target!="example.com:443" { t.Fatalf("target = %q, want example.com:443",got.target) }
}
