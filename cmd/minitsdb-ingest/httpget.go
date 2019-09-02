package main

import (
	"bufio"
	"net/http"
)

func (b *IngestBuffer) ServeHTTPGet(writer http.ResponseWriter, request *http.Request) {
	b.Mux.Lock()
	defer b.Mux.Unlock()

	w := bufio.NewWriter(writer)
	defer w.Flush()

	for i := 0; i < len(b.Buffer); i++ {
		b.Buffer[i].Mux.Lock()
		if len(b.Buffer[i].Values) > 0 {
			w.WriteString(b.Buffer[i].Series + "\n")
			w.WriteString(b.Buffer[i].Columns + "\n")
		loop:
			for {
				select {
				case v := <-b.Buffer[i].Values:
					w.WriteString(v + "\n")
				default:
					break loop
				}
			}
		}
		b.Buffer[i].Mux.Unlock()
		if !b.Buffer[i].Active {
			b.Buffer = append(b.Buffer[:i], b.Buffer[i+1:]...)
			i--
		}
	}
}
