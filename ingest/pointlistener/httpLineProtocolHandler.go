package pointlistener

import (
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/martin2250/minitsdb/ingest"
)

// HTTPLineProtocolHandlergo handles http POST requests and stores incoming points to a point sink
type HTTPLineProtocolHandlergo struct {
	Sink ingest.PointSink
}

// ServeHTTP processes a POST request with line protocol data
func (h HTTPLineProtocolHandlergo) ServeHTTP(writer http.ResponseWriter, request *http.Request) {

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
		point, err := ingest.ParsePoint(line)

		if err != nil {
			log.Printf("point err: %v\n", err)
			return
		}

		h.Sink.AddPoint(point)
	}

	writer.WriteHeader(204)
}
