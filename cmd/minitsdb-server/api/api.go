package api

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api/queryhandler"
	"github.com/martin2250/minitsdb/minitsdb"
	log "github.com/sirupsen/logrus"
)

func Register(db *minitsdb.Database, r *mux.Router) {
	r.Handle("/test", handleTest{})
	r.Handle("/query", queryhandler.New(db, log.New()))
	r.Handle("/list", handleList{db: db})
}
