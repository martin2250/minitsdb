package api

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
	r.Handle("/test", handleTest{})
	r.Handle("/query", newHandleQuery(db, add))
	r.Handle("/list", handleList{db: db})
}
