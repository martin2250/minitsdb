package api

import (
	"github.com/gorilla/mux"
	"github.com/martin2250/minitsdb/database"
)

func Register(db *database.Database, r *mux.Router) {
	r.Handle("/test", handleTest{})
	r.Handle("/query", newHandleQuery(db))
	r.Handle("/list", handleList{db: db})
}
