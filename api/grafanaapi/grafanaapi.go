package grafanaapi

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/database"
)

type Executor interface {
	Execute() error
}

type ExecutorAdder interface {
	Add(e Executor)
}

func Register(db *database.Database, r *mux.Router, add ExecutorAdder) {
	s := r.PathPrefix("/grafana/").Subrouter()
	s.Handle("/test", handleTest{})
	s.Handle("/query", handleQuery{db: db, add: add})
	s.Handle("/list", handleList{db: db})
}
