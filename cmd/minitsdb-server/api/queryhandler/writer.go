package queryhandler

import (
	"io"
	"sync"
)

type httpQueryResultWriter struct {
	output io.Writer
	mux    sync.Mutex
}
