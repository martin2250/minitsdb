package pointlistener

import (
	"bufio"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"github.com/sirupsen/logrus"
	"net"
)

// TCPLineProtocolListener listens for TCP connections, receives points in line protocol format and stores them in the sink
type TCPListener struct {
	sink    chan<- lineprotocol.Point
	address string
}

func NewTCPListener(sink chan<- lineprotocol.Point, address string) TCPListener {
	return TCPListener{
		sink:    sink,
		address: address,
	}
}

// Listen loops endlessly, accepting tcp connections
func (l *TCPListener) Listen() error {
	listener, err := net.Listen("tcp", l.address)

	if err != nil {
		return err
	}

	defer listener.Close()

	for {
		conn, err := listener.Accept()

		if err != nil {
			return err
		}

		go l.handleTCP(conn)
	}
}

// ServeHTTP processes a POST request with line protocol data
func (l *TCPListener) handleTCP(conn net.Conn) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	// todo: this should not interpret the last line sent when the connection is closed
	// instead only interpret lines that actually end with \n
	scanner.Buffer(nil, 1024*16)

	defer func() {
		if r := recover(); r != nil {
			logrus.WithFields(logrus.Fields{"panic": r, "remote": conn.RemoteAddr}).Warning("tcp line protocol panic")
		}
	}()

	for scanner.Scan() {
		point, err := lineprotocol.Parse(scanner.Text())

		if err != nil {
			logrus.WithFields(logrus.Fields{"error": err, "remote": conn.RemoteAddr}).Warning("tcp line protocol error")
		}

		l.sink <- point
	}

	if err := scanner.Err(); err != nil {
		logrus.WithFields(logrus.Fields{"error": err, "remote": conn.RemoteAddr}).Warning("tcp line protocol error")
	}
}
