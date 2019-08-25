package queryhandler

import (
	"github.com/martin2250/minitsdb/minitsdb"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"sync"
)

// QueryClusterReceiver
type Query struct {
	Series  *minitsdb.Series
	Columns []minitsdb.QueryColumn
	Sink    *httpQueryResultWriter
	Done    *sync.WaitGroup
}

// QueryClusterParameters is separate struct so we can use it as a map index
type QueryClusterParameters struct {
	Series   *minitsdb.Series
	Range    TimeRange
	TimeStep int64
}

// QueryCluster is used to collect multiple API queries to the same Series
// into a single query, which reduces load on the server
type QueryCluster struct {
	Parameters QueryClusterParameters
	Receivers  []Query
}

func (c *QueryCluster) Execute() {

}
