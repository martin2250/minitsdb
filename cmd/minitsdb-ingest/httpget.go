package main

import (
	"bufio"
	"io"
	"net/http"
)

func (b *IngestBuffer) ServeHTTPGet(writer http.ResponseWriter, request *http.Request) {
	b.Mux.Lock()
	defer b.Mux.Unlock()

	w := bufio.NewWriter(writer)
	defer w.Flush()

	for i := 0; i < len(b.Buffer); i++ {
		b.Buffer[i].Mux.Lock()
		if b.Buffer[i].Available {
			w.WriteString(b.Buffer[i].Series + "\n")
			w.WriteString(b.Buffer[i].Columns + "\n")
			io.Copy(w, b.Buffer[i].File)
			b.Buffer[i].Available = false
		}
		b.Buffer[i].Mux.Unlock()

		if !b.Buffer[i].Active {
			b.Buffer = append(b.Buffer[:i], b.Buffer[i+1:]...)
			i--
		}
	}
}
