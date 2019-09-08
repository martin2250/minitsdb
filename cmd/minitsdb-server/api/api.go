package api

import (
	"context"
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api/queryhandler"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/sirupsen/logrus"
	"net/http"
	"time"
)

type Config struct {
	Address      string
	ServeTimeout time.Duration

	WaitTimeout time.Duration
	MaxPoints   int64
}

func Start(db *minitsdb.Database, conf Config, shutdown chan struct{}) {
	r := mux.NewRouter() // move this out of the if block when more handlers are added

	r.Handle("/test", handleTest{})
	r.Handle("/query", queryhandler.New(db))
	r.Handle("/list", handleList{db: db})

	srv := &http.Server{
		Addr:    conf.Address,
		Handler: r,

		ReadTimeout:  conf.ServeTimeout,
		WriteTimeout: conf.ServeTimeout,
		IdleTimeout:  conf.ServeTimeout,
	}

	go func() {
		err := srv.ListenAndServe()
		if err != http.ErrServerClosed {
			logrus.WithError(err).Error("HTTP server failed, shutting down")
			close(shutdown)
		}
	}()
	<-shutdown
	srv.Shutdown(context.TODO())
}
