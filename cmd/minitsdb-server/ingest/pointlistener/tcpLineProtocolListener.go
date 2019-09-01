package pointlistener

import (
	"bufio"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/lineprotocol"
	"github.com/sirupsen/logrus"
	"net"
)

// TCPLineProtocolListener listens for TCP connections, receives points in line protocol format and stores them in the sink
type TCPListener struct {
	sink    chan<- lineprotocol.Point
	db      *minitsdb.Database
	address string
}

func NewTCPListener(db *minitsdb.Database, sink chan<- lineprotocol.Point, address string) TCPListener {
	return TCPListener{
		sink:    sink,
		db:      db,
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
	parser := lineprotocol.NewParser(l.db, l.sink)

	defer func() {
		if r := recover(); r != nil {
			logrus.WithFields(logrus.Fields{"panic": r, "remote": conn.RemoteAddr}).Warning("tcp line protocol panic")
		}
	}()

	for scanner.Scan() {
		err := parser.ParseLine(scanner.Text())

		if err != nil {
			logrus.WithFields(logrus.Fields{"error": err, "remote": conn.RemoteAddr}).Warning("tcp line protocol error")
		}
	}

	if err := scanner.Err(); err != nil {
		logrus.WithFields(logrus.Fields{"error": err, "remote": conn.RemoteAddr}).Warning("tcp line protocol error")
	}
}
