package main

import (
	"bufio"
	"net/http"
)

func (b *IngestBuffer) ServeHTTPPut(writer http.ResponseWriter, request *http.Request) {
	scanner := bufio.NewScanner(request.Body)

	em := LineProtocolEmulator{
		Buffer: b,
	}
	defer em.Reset()

	for scanner.Scan() {
		em.Parse(scanner.Text())
	}
}
