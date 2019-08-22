package main

import (
	"github.com/gorilla/mux"
	"github.com/jessevdk/go-flags"
	"github.com/martin2250/minitsdb/api"
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
	log "github.com/sirupsen/logrus"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// go tool pprof -web ___go_build_main_go 973220726.pprof
// GOOS=linu GOARCH=arm GOARM=7 go build -ldflags="-w -s" .

func main() {
	//tmpfile, _ := ioutil.TempFile("", "*.pprof")
	//log.Info(tmpfile.Name())
	//pprof.StartCPUProfile(tmpfile)
	//defer func() {
	//	pprof.StopCPUProfile()
	//	cmd := exec.Command("go", "tool", "pprof", "-web", "/tmp/___go_build_main_go", tmpfile.Name())
	//	cmd.Run()
	//	os.Remove(tmpfile.Name())
	//}()

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

	r := mux.NewRouter()
	routerApi := r.PathPrefix("/api/").Subrouter()

	srv := &http.Server{
		Addr: "0.0.0.0:8080",
		//WriteTimeout: time.Second * 15,
		//ReadTimeout:  time.Second * 15,
		//IdleTimeout:  time.Second * 60,
		Handler: r,
	}

	channelIngestPoints := make(chan ingest.Point, 1024*128)
	channelAssociatedPoints := make(chan series.AssociatedPoint, 1024*128)

	httpl := pointlistener.HTTPLineProtocolHandler{
		Sink: ingest.ChanPointSink(channelIngestPoints),
	}
	routerApi.Handle("/insert", httpl)

	tcpl := pointlistener.TCPLineProtocolListener{
		Sink: ingest.ChanPointSink(channelIngestPoints),
	}

	go tcpl.Listen(8001)

	api.Register(&db, routerApi)

	timerFlush := time.Tick(1 * time.Second)

	go srv.ListenAndServe()

	go func() {
		for {
			p := <-channelIngestPoints
			indices := db.FindSeries(p.Tags)
			if len(indices) != 1 {
				continue
			}
			ps, err := indices[0].ConvertPoint(p)
			if err != nil {
				continue
			}
			channelAssociatedPoints <- series.AssociatedPoint{
				Point:  ps,
				Series: indices[0],
			}
		}
	}()

LoopMain:
	for {
		select {
		case <-timerFlush:
			db.FlushSeries()
		case p := <-channelAssociatedPoints:
			err = p.Series.InsertPoint(p.Point)
			if err != nil {
				log.Println(err)
			}
			if p.Series.CheckFlush() {
				p.Series.Flush()
			}
		case <-shutdown:
			break LoopMain
		}
	}

	log.Info("Flushing buffers")

	for _, s := range db.Series {
		s.FlushAll()
	}

	log.Info("Terminating")
}
