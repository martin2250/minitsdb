package main

import (
	"bufio"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net"
)

func (b *IngestBuffer) ListenTCP(address string) error {
	listener, err := net.Listen("tcp", address)

	if err != nil {
		return err
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()

		if err != nil {
			return err
		}

		go b.HandleTCP(conn)
	}
}

func (b *IngestBuffer) HandleTCP(conn net.Conn) error {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		p, err := lineprotocol.Parse(scanner.Text())

		b.Mux.Lock()
		if err == nil {
			b.Points.PushBack(p)
		} else {
			b.AddError(scanner.Text())
		}
		b.Mux.Unlock()
	}

	return scanner.Err()
}
