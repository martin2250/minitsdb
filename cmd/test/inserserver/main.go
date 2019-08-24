package main

import (
	"fmt"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest/pointlistener"
	"time"

	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
)

func main() {
	buffer := ingest.NewPointList()

	tcpl := pointlistener.TCPLineProtocolListener{
		Sink: &buffer,
	}

	go tcpl.Listen(8001)

	for {
		point, ok := buffer.GetPoint()

		if ok {
			_, ok := point.Tags["shit"]
			if ok {
				fmt.Println("k")
			}
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}
