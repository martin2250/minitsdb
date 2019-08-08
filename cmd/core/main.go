package main

import (
	"fmt"
	"github.com/martin2250/minitsdb/api"
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strconv"
	"time"
)

func main() {
	db, err := database.NewDatabase("/home/martin/Desktop/minitsdb_database")

	if err != nil {
		panic(err)
	}

	//buffer := ingest.NewPointList()
	buffer := ingest.NewPointFifo()

	tcpl := pointlistener.TCPLineProtocolListener{
		Sink: &buffer,
	}

	go tcpl.Listen(8001)

	httpl := pointlistener.HTTPLineProtocolHandler{
		Sink: &buffer,
	}
	http.Handle("/insert", httpl)

	api := api.NewDatabaseAPI(&db)

	http.Handle("/query/", api)

	go http.ListenAndServe(":8080", nil)

	for {
		// read point
		point, ok1 := buffer.GetPoint()

		if ok1 {
			err = db.InsertPoint(point)

			if err != nil {
				log.Println(err)
			}
		}

		// serve query
		q, ok2 := api.GetQuery()

		if ok2 {
			runtime.GC()
			q.Writer.Write([]byte("starting processing"))
			for {
				vals, err := q.ReadNext()

				if err == nil {
					for i := range vals.Time {
						io.WriteString(q.Writer, strconv.FormatInt(vals.Time[i], 10))
						for j := range q.Param.Columns {
							q.Writer.Write([]byte{0x20}) // spaaaaaacee!
							io.WriteString(q.Writer, strconv.FormatInt(vals.Values[j][i], 10))
						}
						q.Writer.Write([]byte{0x0A}) // newline
					}
				} else {
					fmt.Fprint(q.Writer, err)
					break
				}
			}
			q.Done <- struct{}{}
		}

		if !ok1 && !ok2 {
			time.Sleep(time.Millisecond)
		}
	}
}
