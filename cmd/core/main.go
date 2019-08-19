package main

import (
	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	"github.com/martin2250/minitsdb/api"
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
	log "github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
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

	// add shutdown handler
	sigs := make(chan os.Signal)
	shutdown := make(chan bool)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		shutdown <- true
		log.Info("Received signal to shutdown")
		// force after timeout
		time.Sleep(300 * time.Millisecond)
	}()

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
	routerApi := r.PathPrefix("/api/").Subrouter()

	srv := &http.Server{
		Addr: "0.0.0.0:8080",
		//WriteTimeout: time.Second * 15,
		//ReadTimeout:  time.Second * 15,
		//IdleTimeout:  time.Second * 60,
		Handler: r,
	}

	httpl := pointlistener.HTTPLineProtocolHandler{
		Sink: &buffer,
	}
	routerApi.Handle("/insert", httpl)

	api.Register(&db, routerApi)

	//api := api.NewDatabaseAPI(&db)
	//
	//http.Handle("/query/", api)

	go srv.ListenAndServe()

LoopMain:
	for {
		// insert new points
		for {
			point, ok := buffer.GetPoint()
			if !ok {
				break
			}
			err = db.InsertPoint(point)
			if err != nil {
				log.Println(err)
			}
		}

		select {
		case <-time.Tick(10 * time.Millisecond):
		case <-shutdown:
			break LoopMain
		}
	}

	log.Info("Flushing buffers")

	for _, s := range db.Series {
		s.Flush()
	}
}
