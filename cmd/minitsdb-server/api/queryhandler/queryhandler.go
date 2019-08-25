package queryhandler

import (
	"github.com/martin2250/minitsdb/minitsdb"
	log "github.com/sirupsen/logrus"
	"sync"
)

type queryHandler struct {
	db  *minitsdb.Database
	log *log.Logger

	pendingQueries map[QueryClusterParameters]*QueryCluster
	mux            sync.Mutex
}

func New(db *minitsdb.Database, log *log.Logger) *queryHandler {
	return &queryHandler{
		db:             db,
		log:            log,
		pendingQueries: make(map[QueryClusterParameters]*QueryCluster),
		mux:            sync.Mutex{},
	}
}
