package pointlistener

import (
	"bufio"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/lineprotocol"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/sirupsen/logrus"
	"log"
	"net/http"
)

// HTTPHandler handles http POST requests and stores incoming points to a point sink
type HTTPHandler struct {
	sink chan<- lineprotocol.Point
	db   *minitsdb.Database
}

func NewHTTPHandler(db *minitsdb.Database, sink chan<- lineprotocol.Point) HTTPHandler {
	return HTTPHandler{
		sink: sink,
		db:   db,
	}
}

// ServeHTTP processes a POST request with line protocol data
func (h HTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		log.Printf("invalid method: %s\n", request.Method)
		return
	}

	scanner := bufio.NewScanner(request.Body)
	parser := lineprotocol.NewParser(h.db, h.sink)

	defer func() {
		if r := recover(); r != nil {
			logrus.WithFields(logrus.Fields{"panic": r, "remote": request.RemoteAddr}).Warning("http line protocol panic")
		}
	}()

	for scanner.Scan() {
		err := parser.ParseLine(scanner.Text())

		if err != nil {
			logrus.WithFields(logrus.Fields{"error": err, "remote": request.RemoteAddr}).Warning("http line protocol error")
		}
	}

	if err := scanner.Err(); err != nil {
		logrus.WithFields(logrus.Fields{"error": err, "remote": request.RemoteAddr}).Warning("http line protocol error")
	}

	writer.WriteHeader(204)
}
