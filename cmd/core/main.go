package main

import (
	fifo "github.com/foize/go.fifo"
	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	"github.com/martin2250/minitsdb/api/grafanaapi"
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
	log "github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ExecutorQueque struct {
	executors *fifo.Queue
}

func NewExecutorQueque() ExecutorQueque {
	return ExecutorQueque{
		executors: fifo.NewQueue(),
	}
}

//AddQuery enqueues a query to be executed on the next query cycle
func (q ExecutorQueque) Add(e grafanaapi.Executor) {
	q.executors.Add(e)
}

//GetQuery returns the oldest query from the query buffer
//returns false if no query is available
func (q ExecutorQueque) Get() (grafanaapi.Executor, bool) {
	ei := q.executors.Next()

	if ei == nil {
		return nil, false
	}

	e, ok := ei.(grafanaapi.Executor)

	if !ok {
		return nil, false
	}

	return e, true
}

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
	api := r.PathPrefix("/api/").Subrouter()

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
	api.Handle("/insert", httpl)

	gqueue := NewExecutorQueque()

	grafanaapi.Register(&db, api, gqueue)

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

		// serve queries
		if gqueue.executors.Len() > 0 {
			// wait for more queries to arrive
			// todo: improve this by checking if there are queries waiting in api
			time.Sleep(10 * time.Millisecond)

			// wait until all queries are finished but run them in parallel
			var wg sync.WaitGroup
			for {
				e, ok := gqueue.Get()
				if ok {
					wg.Add(1)
					go func() {
						e.Execute()
						wg.Done()
					}()
				}
			}
			wg.Wait()
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
