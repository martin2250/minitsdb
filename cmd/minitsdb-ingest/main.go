package main

import (
	"net/http"
	"sync"
)

type PointCollection struct {
	Series  string
	Columns string
	Values  chan string
	Active  bool
	Mux     sync.Mutex
}

type IngestBuffer struct {
	Buffer []PointCollection
	Mux    sync.Mutex
}

func main() {
	buf := IngestBuffer{}

	go func() {
		err := buf.ListenTCP(":10000")
		if err != nil {
			panic(err)
		}
	}()

	http.HandleFunc("/insert", func(w http.ResponseWriter, r *http.Request) { buf.ServeHTTPPut(w, r) })
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) { buf.ServeHTTPGet(w, r) })
	http.ListenAndServe(":10080", nil)
}
