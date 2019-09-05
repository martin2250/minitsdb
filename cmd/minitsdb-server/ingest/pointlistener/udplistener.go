package pointlistener

import (
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net"
)

func ListenUDP(sink chan<- lineprotocol.Point, address string) error {
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

		go func(text string, sink chan<- lineprotocol.Point) {
			p, err := lineprotocol.Parse(text)

			if err == nil {
				sink <- p
			}
		}(string(buf[:n]), sink)
	}
}
