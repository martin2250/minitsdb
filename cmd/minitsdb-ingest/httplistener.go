package main

import (
	"bufio"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net/http"
)

func (b *IngestBuffer) ServeHTTPPut(writer http.ResponseWriter, request *http.Request) {
	scanner := bufio.NewScanner(request.Body)

	for scanner.Scan() {
		p, err := lineprotocol.Parse(scanner.Text())

		b.Mux.Lock()
		if err == nil {
			b.Points.PushBack(p)
		} else {
			b.AddError(scanner.Text())
		}
		b.Mux.Unlock()
	}
}
