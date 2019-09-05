package main

import (
	"bufio"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net/http"
)

func (b *IngestBuffer) ServeHTTPRead(writer http.ResponseWriter, request *http.Request, discard bool) {
	b.Mux.Lock()
	defer b.Mux.Unlock()

	w := bufio.NewWriter(writer)
	defer w.Flush()

	for e := b.Points.Front(); e != nil; e = e.Next() {
		point := e.Value.(lineprotocol.Point)
		w.WriteString(point.String())
		w.WriteByte('\n')
	}

	if discard {
		b.Points.Init()
	}
}

func (b *IngestBuffer) ServeHTTPErrors(writer http.ResponseWriter, request *http.Request) {
	b.Mux.Lock()
	defer b.Mux.Unlock()

	w := bufio.NewWriter(writer)
	defer w.Flush()

	for e := b.Errors.Back(); e != nil; e = e.Prev() {
		err := e.Value.(Error)
		w.WriteString(err.Time.String())
		w.WriteString(": ")
		w.WriteString(err.Text)
		w.WriteByte('\n')
	}
}
