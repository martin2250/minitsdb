package grafanaapi

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/database"
)

func Register(db *database.Database, r *mux.Router) {
	s := r.PathPrefix("/grafana/").Subrouter()
	s.Handle("/test", handleTest{})
	s.Handle("/query", handleQuery{db: db})
	s.Handle("/list", handleList{db: db})
}

type APIQuery interface {
	Execute() error
}
