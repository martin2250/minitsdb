package pointlistener

import (
	ingest2 "github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

// HTTPLineProtocolHandler handles http POST requests and stores incoming points to a point sink
type HTTPLineProtocolHandler struct {
	Sink ingest2.PointSink
}

// ServeHTTP processes a POST request with line protocol data
func (h HTTPLineProtocolHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		log.Printf("invalid method: %s\n", request.Method)
		return
	}

	data, err := ioutil.ReadAll(request.Body)

	if err != nil {
		log.Printf("http read err: %v\n", err)
		return
	}

	text := string(data)

	lines := strings.Split(text, "\n")

	for _, line := range lines {
		point, err := ingest2.ParsePoint(line)

		if err != nil {
			log.Printf("point err: %v\n", err)
			return
		}

		h.Sink.AddPoint(point)
	}

	writer.WriteHeader(204)
}
