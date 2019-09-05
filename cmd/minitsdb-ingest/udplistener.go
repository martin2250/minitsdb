package main

import (
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net"
)

func (b *IngestBuffer) ListenUDP(address string) error {
	addr, err := net.ResolveUDPAddr("udp", address)

	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", addr)

	if err != nil {
		return err
	}

	defer conn.Close()

	buf := make([]byte, 1024*64)

	for {
		n, _, err := conn.ReadFromUDP(buf)

		if err != nil {
			return err
		}

		go b.HandleUDP(string(buf[:n]))
	}
}

func (b *IngestBuffer) HandleUDP(line string) {
	p, err := lineprotocol.Parse(line)

	b.Mux.Lock()
	if err == nil {
		b.Points.PushBack(p)
	} else {
		b.AddError(line)
	}
	b.Mux.Unlock()
}
