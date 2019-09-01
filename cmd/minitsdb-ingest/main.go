package main

import (
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

}
