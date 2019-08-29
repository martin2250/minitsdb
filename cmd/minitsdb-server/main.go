package main

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest/pointlistener"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/rossmcdonald/telegram_hook"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/http/pprof"
	"time"
)

// go tool pprof -web ___go_build_main_go 973220726.pprof
// GOOS=linu GOARCH=arm GOARM=7 go build -ldflags="-w -s" .

func main() {
	// command line
	opts := readCommandLineOptions()

	// profiling
	if opts.CpuProfilePath != "" {
		startCpuProfile(opts.CpuProfilePath)
		defer stopCpuProfile(opts.CpuProfilePlot)
	}

	if opts.TracePath != "" {
		startTrace(opts.TracePath)
		defer stopTrace()
	}

	// configuration
	conf := readConfigurationFile(opts.ConfigPath)
	if conf.DatabasePath != "" {
		opts.DatabasePath = conf.DatabasePath
	}

	if conf.Telegram != nil {
		hook, err := telegram_hook.NewTelegramHook(
			conf.Telegram.AppName,
			conf.Telegram.AuthToken,
			conf.Telegram.ChatID,
			telegram_hook.WithAsync(true),
			telegram_hook.WithTimeout(30*time.Second),
		)
		if err != nil {
			log.WithError(err).Fatalf("failed to create telegram log hook")
		}
		log.AddHook(hook)
	}

	// shutdown
	shutdown := make(chan bool)
	go gracefulShutdown(shutdown, conf.ShutdownTimeout)

	// database
	db := loadDatabase(opts.DatabasePath)

	// ingestion
	ingestPoints := make(chan ingest.Point, conf.IngestBufferCapacity)
	associatedPoints := make(chan minitsdb.AssociatedPoint, conf.IngestBufferCapacity)
	for i := uint(0); i < conf.IngestionWorkerCount; i++ {
		go associatePoints(ingestPoints, associatedPoints, &db)
	}

	// http
	if conf.ApiPath != "" {
		r := mux.NewRouter() // move this out of the if block when more handlers are added

		routerApi := r.PathPrefix(conf.ApiPath).Subrouter()
		api.Register(&db, routerApi)

		httpl := pointlistener.HTTPLineProtocolHandler{
			Sink: ingest.ChanPointSink(ingestPoints),
		}
		routerApi.Handle("/insert", httpl)
		r.PathPrefix("/debug/").HandlerFunc(pprof.Index)

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
		tcpl := pointlistener.TCPLineProtocolListener{
			Sink:    ingest.ChanPointSink(ingestPoints),
			Address: conf.TcpListenAddress,
		}
		go shutdownOnError(tcpl.Listen, shutdown, conf.ShutdownTimeout, "TCP listener failed")
	}

	timerFlush := time.Tick(1 * time.Second)

	for i := range db.Series {
		lastBucket := &db.Series[i].FirstBucket
		primary := true
		logds := log.WithFields(log.Fields{"series": &db.Series[i].Tags})
		logds.Info("downsampling series")

		for ib := range db.Series[i].LastBuckets {
			logds.WithFields(log.Fields{"bucketid": ib, "primary": primary}).Info("downsampling bucket")

			ds := minitsdb.NewDownsampler(&db.Series[i], lastBucket, &db.Series[i].LastBuckets[ib], primary)

			primary = false
			lastBucket = &db.Series[i].LastBuckets[ib]

			err := ds.Run()
			if err != nil {
				logds.WithError(err).Error("error while downsampling")
				break
			}
		}
	}

LoopMain:
	for {
		select {
		case <-timerFlush:
			db.FlushSeries()
		case p := <-associatedPoints:
			err := p.Series.InsertPoint(p.Point)
			if err != nil {
				log.WithError(err).Warning("Insert Failed")
				continue
			}
			if p.Series.CheckFlush() {
				p.Series.Flush()
			}
		case <-shutdown:
			break LoopMain
		}
	}

	// todo: shut down ingestion pipeline in order

	log.Info("Flushing buffers")

	for _, s := range db.Series {
		s.FlushAll()
	}

	log.Info("Terminating")
}
