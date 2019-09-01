package main

import (
	"bufio"
	"github.com/martin2250/minitsdb/minitsdb/lineprotocol"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"

	fifo "github.com/foize/go.fifo"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
)

// Buffer is the IngestBuffer instance that holds the main program state
var Buffer IngestBuffer

// IngestBuffer holds the main program state
type IngestBuffer struct {
	Points *fifo.Queue
}

// NewIngestBuffer creates new IngestBuffer
func NewIngestBuffer() IngestBuffer {
	return IngestBuffer{
		Points: fifo.NewQueue(),
	}
}

// IngestBufferRPC methods to access IngestBuffer via RPC
type IngestBufferRPC struct {
	buffer *IngestBuffer
}

// PopPoint deletes the oldest point in the buffer and stores it in *reply
func (b *IngestBufferRPC) PopPoint(arg int, reply *lineprotocol.Point) error {
	pointi := b.buffer.Points.Next()

	if pointi == nil {
		return io.EOF
	}

	point, ok := pointi.(*lineprotocol.Point)

	if !ok {
		return io.EOF
	}

	*reply = *point

	return nil
}

// InsertLine parses a line to a point and inserts it into the buffer
func (b *IngestBuffer) InsertLine(line string) error {
	point, err := ingest.ParsePoint(line)

	if err != nil {
		return err
	}

	b.Points.Add(&point)

	return nil
}

func handleHTTPInsertLine(writer http.ResponseWriter, request *http.Request) {
	reader := bufio.NewReader(request.Body)

	for {
		line, _, err := reader.ReadLine() // todo: check isPrefix and handle accordingly

		if err == io.EOF {
			break
		} else if err != nil {
			continue
		}

		err = Buffer.InsertLine(string(line))
	}
}

func main() {
	Buffer = NewIngestBuffer()

	rpc.Register(&IngestBufferRPC{&Buffer})
	rpc.HandleHTTP()

	l, err := net.Listen("tcp", ":2002")

	if err != nil {
		log.Fatal("listen error:", err)
	}

	http.HandleFunc("/insert/line", handleHTTPInsertLine)
	// http.HandleFunc("/insert/bin", handleHTTPInsertBinary) // to be added later, takes binary values and parses them according to a config file

	http.Serve(l, nil)
}
