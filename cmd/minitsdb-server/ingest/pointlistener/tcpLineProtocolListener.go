package pointlistener

import (
	"bufio"
	ingest2 "github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
	"io"
	"log"
	"net"
	"strings"
)

// TCPLineProtocolListener listens for TCP connections, receives points in line protocol format and stores them in the sink
type TCPLineProtocolListener struct {
	Sink    ingest2.PointSink
	Address string
}

// Listen loops endlessly, accepting tcp connections
func (tl TCPLineProtocolListener) Listen() error {
	l, err := net.Listen("tcp", tl.Address)

	if err != nil {
		return err
	}

	defer l.Close()

	for {
		c, err := l.Accept()

		if err != nil {
			return err
		}

		go tl.handleTCP(c)
	}
}

// ServeHTTP processes a POST request with line protocol data
func (tl TCPLineProtocolListener) handleTCP(c net.Conn) {
	defer c.Close()

	r := bufio.NewReader(c)

	for {
		line, err := r.ReadString('\n')

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Printf("conn err: %v\n", err)
			break
		}

		point, err := ingest2.ParsePoint(strings.TrimSpace(line))

		// don't close connection on parse error
		if err != nil {
			log.Printf("point err: %v\n", err)
			continue
		}

		tl.Sink.AddPoint(point)
	}
}
