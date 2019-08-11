package main

import (
	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	"github.com/martin2250/minitsdb/api/grafanaapi"
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
	log "github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"time"
)

func main() {
	opts := struct {
		DbPath string `short:"d" long:"database" description:"database path"`
	}{
		DbPath: "/home/martin/Desktop/minitsdb_database",
	}
	_, err := flags.Parse(&opts)

	if err != nil {
		return
	}

	// load database
	log.WithField("path", opts.DbPath).Info("Loading database")
	db, err := database.NewDatabase(opts.DbPath)

	if err != nil {
		log.WithField("error", err.Error()).Fatal("Failed to load database")
	}

	// set up ingestion
	buffer := ingest.NewPointFifo()

	tcpl := pointlistener.TCPLineProtocolListener{
		Sink: &buffer,
	}

	go tcpl.Listen(8001)

	r := mux.NewRouter()
	api := r.PathPrefix("/api/").Subrouter()

	srv := &http.Server{
		Addr:         "0.0.0.0:8080",
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      r,
	}

	httpl := pointlistener.HTTPLineProtocolHandler{
		Sink: &buffer,
	}
	api.Handle("/insert", httpl)

	grafanaapi.Register(&db, api)

	//api := api.NewDatabaseAPI(&db)
	//
	//http.Handle("/query/", api)

	go srv.ListenAndServe()

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
		//q, ok2 := api.GetQuery()
		//
		//if ok2 {
		//	runtime.GC()
		//	for {
		//		vals, err := q.Query.ReadNext()
		//
		//		if err == nil {
		//			q.Data <- vals
		//		} else {
		//			break
		//		}
		//	}
		//	close(q.Data)
		//}

		if !ok1 /*&& !ok2*/ {
			time.Sleep(time.Millisecond)
		}
	}
}
