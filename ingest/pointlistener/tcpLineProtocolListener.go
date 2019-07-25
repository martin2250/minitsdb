package pointlistener

import (
	"bufio"
	"fmt"
	"log"
	"net"

	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointsource"
)

// TCPLineProtocolListener listens for TCP connections, receives points in line protocol format and stores them in the sink
type TCPLineProtocolListener struct {
	Sink pointsource.PointSink
}

// Listen loops endlessly, accepting tcp connections
func (tl TCPLineProtocolListener) Listen(port uint16) error {
	l, err := net.Listen("tcp", fmt.Sprintf(":%d", port))

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

		if err != nil {
			log.Printf("conn err: %v\n", err)
			break
		}

		point, err := ingest.ParsePoint(line)

		// don't close connection on parse error
		if err != nil {
			log.Printf("point err: %v\n", err)
			continue
		}

		tl.Sink.AddPoint(point)
	}
}
