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

func New() queryHandler {
	return queryHandler{}
}
