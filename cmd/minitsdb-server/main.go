package main

import (
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest/pointlistener"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"github.com/sirupsen/logrus"
	"log"
	"net/http"
	_ "net/http/pprof"
	"time"
)

// go tool pprof -web ___go_build_main_go 973220726.pprof
// ( cd ~/go/src/github.com/martin2250/minitsdb/cmd/minitsdb-server && GOOS=linux GOARCH=arm GOARM=7 go build -ldflags="-w -s" . && scp minitsdb-server martin@192.168.2.91:/home/martin/minitsdb/minitsdb-server )
// ( cd ~/go/src/github.com/martin2250/minitsdb/cmd/minitsdb-ingest && GOOS=linux GOARCH=arm GOARM=7 go build -ldflags="-w -s" . && scp minitsdb-ingest martin@192.168.2.91:/home/martin/minitsdb/minitsdb-ingest )
// ( cd ~/go/src/github.com/martin2250/minitsdb/cmd/minitsdb-system && GOOS=linux GOARCH=arm GOARM=7 go build -ldflags="-w -s" . && scp minitsdb-system martin@192.168.2.91:/home/martin/minitsdb/minitsdb-system )

func main() {
	// command line
	opts := readCommandLineOptions()

	// profiling
	if opts.Profile != "" {
		defer debugStartProfile(opts.Profile, opts.ProfilePath).Stop()
	}

	// configuration
	var conf Configuration
	if opts.ConfigPath == "" {
		conf = ConfigDefault
	} else {
		conf = readConfigurationFile(opts.ConfigPath)
	}

	if opts.DatabasePath != "" {
		conf.DatabasePath = opts.DatabasePath
	}

	// logrus hooks
	logAddBackends(conf)

	// shutdown
	shutdown := make(chan struct{})
	go listenShutdown(shutdown, conf.ShutdownTimeout)

	// database
	db := loadDatabase(conf.DatabasePath)

	// ingestion
	ingestPoints := make(chan lineprotocol.Point, conf.Ingest.Buffer)

	// http
	if conf.API.Address != "" {
		go api.Start(&db, conf.API, shutdown)
	}

	// ingest
	go pointlistener.ReadIngestServer(ingestPoints, conf.Ingest.Servers, shutdown)

	// debug/pprof interface todo: make optional
	go func() {
		log.Println(http.ListenAndServe(":6060", nil))
	}()

	timerTick := time.Tick(1 * time.Second)

LoopMain:
	for {
		select {
		case <-timerTick:
			db.Downsample()
			db.FlushSeries()

		case point, ok := <-ingestPoints:
			if !ok {
				break LoopMain
			}
			s, p, err := db.AssociatePoint(point)

			if err == nil {
				err = s.InsertPoint(p)
			}

			if err != nil {
				logrus.WithError(err).WithField("point", point).Warning("Insert Failed")
				continue
			} else {
				if s.CheckFlush() {
					s.Flush()
				}
			}
		}
	}

	logrus.Info("Flushing buffers")

	for _, s := range db.Series {
		s.FlushAll()
	}

	logrus.Info("Terminating")
}
