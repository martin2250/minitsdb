package main

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest/pointlistener"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"github.com/sirupsen/logrus"
	"net/http"
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
	conf := readConfigurationFile(opts.ConfigPath)
	if conf.DatabasePath != "" {
		opts.DatabasePath = conf.DatabasePath
	}

	// logrus hooks
	logAddBackends(conf)

	// shutdown
	shutdown := make(chan bool)
	go gracefulShutdown(shutdown, conf.ShutdownTimeout)

	// database
	db := loadDatabase(opts.DatabasePath)

	// ingestion
	ingestPoints := make(chan lineprotocol.Point, conf.IngestBufferCapacity)

	// http
	if conf.ApiPath != "" {
		r := mux.NewRouter() // move this out of the if block when more handlers are added

		routerApi := r.PathPrefix(conf.ApiPath).Subrouter()
		api.Register(&db, routerApi)

		httpl := pointlistener.NewHTTPHandler(ingestPoints)
		routerApi.Handle("/insert", &httpl)

		srv := &http.Server{
			Addr:    conf.ApiListenAddress,
			Handler: r,

			ReadHeaderTimeout: conf.ApiTimeout,
			ReadTimeout:       conf.ApiTimeout,
			WriteTimeout:      conf.ApiTimeout,
			IdleTimeout:       conf.ApiTimeout,
		}
		go shutdownOnError(srv.ListenAndServe, shutdown, conf.ShutdownTimeout, "HTTP server failed")
	}

	// tcp
	if conf.TcpListenAddress != "" {
		tcpl := pointlistener.NewTCPListener(ingestPoints, conf.TcpListenAddress)
		go shutdownOnError(tcpl.Listen, shutdown, conf.ShutdownTimeout, "TCP listener failed")
	}

	// udp
	if conf.UdpListenAddress != "" {
		go pointlistener.ListenUDP(ingestPoints, conf.UdpListenAddress)
	}

	// http read
	if conf.IngestAddress != "" {
		go pointlistener.ReadIngestServer(ingestPoints, conf.IngestAddress)
	}

	timerFlush := time.Tick(1 * time.Second)
	timerDownsample := time.Tick(1 * time.Second)

LoopMain:
	for {
		select {
		case <-timerFlush:
			db.FlushSeries()

		case <-timerDownsample:
			db.Downsample()

		case point := <-ingestPoints:
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

		case <-shutdown:
			break LoopMain
		}
	}

	// todo: shut down ingestion pipeline in order

	logrus.Info("Flushing buffers")

	for _, s := range db.Series {
		s.FlushAll()
	}

	logrus.Info("Terminating")
}
