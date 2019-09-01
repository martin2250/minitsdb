package main

import (
	"bufio"
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

	em := LineProtocolEmulator{
		Buffer: b,
	}
	defer em.Reset()

	for scanner.Scan() {
		em.Parse(scanner.Text())
	}

	return scanner.Err()
}
