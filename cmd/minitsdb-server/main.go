package main

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest/pointlistener"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/lineprotocol"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/pkg/profile"
	"github.com/rossmcdonald/telegram_hook"
	log "github.com/sirupsen/logrus"
	"net/http"
	"time"
)

// go tool pprof -web ___go_build_main_go 973220726.pprof
// ( cd ~/go/src/github.com/martin2250/minitsdb/cmd/minitsdb-server && GOOS=linux GOARCH=arm GOARM=7 go build -ldflags="-w -s" . && scp minitsdb-server martin@192.168.2.91:/home/martin/minitsdbtest/minitsdb-server )

func main() {
	// command line
	opts := readCommandLineOptions()

	// profiling
	if opts.Profile != "" {
		var p func(p *profile.Profile)
		switch opts.Profile {
		case "cpu":
			p = profile.CPUProfile
		case "mem":
			p = profile.MemProfile
		case "mutex":
			p = profile.MutexProfile
		case "block":
			p = profile.BlockProfile
		case "thread":
			p = profile.ThreadcreationProfile
		case "trace":
			p = profile.TraceProfile
		default:
			log.WithField("profile", opts.Profile).Fatal("Unknown profile type")
		}

		popts := []func(p *profile.Profile){p, profile.NoShutdownHook}

		if opts.ProfilePath != "" {
			popts = append(popts, profile.ProfilePath(opts.ProfilePath))
			popts = append(popts, profile.Quiet)
		}

		defer profile.Start(popts...).Stop()
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
	ingestPoints := make(chan lineprotocol.Point, conf.IngestBufferCapacity)

	// http
	if conf.ApiPath != "" {
		r := mux.NewRouter() // move this out of the if block when more handlers are added

		routerApi := r.PathPrefix(conf.ApiPath).Subrouter()
		api.Register(&db, routerApi)

		httpl := pointlistener.NewHTTPHandler(&db, ingestPoints)
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
		tcpl := pointlistener.NewTCPListener(&db, ingestPoints, conf.TcpListenAddress)
		go shutdownOnError(tcpl.Listen, shutdown, conf.ShutdownTimeout, "TCP listener failed")
	}

	timerFlush := time.Tick(1 * time.Second)
	timerDownsample := time.Tick(1 * time.Second)

LoopMain:
	for {
		select {
		case <-timerFlush:
			db.FlushSeries()
		case <-timerDownsample:
			for is := range db.Series {
				for ib := range db.Series[is].Buckets {
					if !db.Series[is].Buckets[ib].Downsample() {
						break
					}
				}
			}
		case p := <-ingestPoints:
			err := p.Series.InsertPoint(storage.Point{Values: p.Values})
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
