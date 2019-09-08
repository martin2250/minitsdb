package queryhandler

import (
	"github.com/martin2250/minitsdb/minitsdb"
	"sync"
)

type queryHandler struct {
	db *minitsdb.Database

	pendingQueries map[QueryClusterParameters]*QueryCluster
	mux            sync.Mutex
}

func New(db *minitsdb.Database) *queryHandler {
	return &queryHandler{
		db:             db,
		pendingQueries: make(map[QueryClusterParameters]*QueryCluster),
		mux:            sync.Mutex{},
	}
}
