package main

import (
	"log"
	"time"

	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
)

func main() {
	db, err := database.NewDatabase("/home/martin/Desktop/minitsdb_database")

	if err != nil {
		panic(err)
	}

	buffer := ingest.NewPointFifo()

	tcpl := pointlistener.TCPLineProtocolListener{
		Sink: &buffer,
	}

	go tcpl.Listen(8001)

	for {
		point, ok := buffer.GetPoint()

		if !ok {
			time.Sleep(time.Millisecond)
			continue
		}

		err := db.InsertPoint(point)

		if err != nil {
			log.Println(err)
		}
	}
}
