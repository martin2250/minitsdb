package main

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/api"
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/ingest/pointlistener"
	"github.com/rossmcdonald/telegram_hook"
	log "github.com/sirupsen/logrus"
	"net/http"
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
	associatedPoints := make(chan series.AssociatedPoint, conf.IngestBufferCapacity)
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

	log.Info("Flushing buffers")

	for _, s := range db.Series {
		s.FlushAll()
	}

	log.Info("Terminating")
}
