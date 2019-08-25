package queryhandler

import (
	"errors"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"io"
	"sync"
)

type QueryResultWriter interface {
	Write(buffer storage.PointBuffer) error
}

// QueryClusterReceiver
type SubQuery struct {
	Series  *minitsdb.Series       // the requested series
	Columns []minitsdb.QueryColumn // and columns

	// interface to the HTTP request
	Done   *sync.WaitGroup
	Cancel chan struct{}
	Sink   QueryResultWriter
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
	SubQueries []SubQuery
}

func (c *QueryCluster) Execute() error {
	var columns []minitsdb.QueryColumn
	for _, subQuery := range c.SubQueries {
		columns = append(columns, subQuery.Columns...)
	}

	query := c.Parameters.Series.Query(minitsdb.Parameters{
		TimeStep: c.Parameters.TimeStep,
		Columns:  columns,
	}, c.Parameters.Range)

	defer func() {
		for _, subQuery := range c.SubQueries {
			subQuery.Done.Done()
		}
	}()

	for {
		buffer, err := query.ReadNext()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if buffer.Cols() != len(columns) {
			// this should never happen, maybe replace with panic or just leave out?
			return errors.New("query returned buffer with different size")
		}

		if buffer.Len() == 0 {
			continue
		}

		var i int
		for _, subQuery := range c.SubQueries {
			_ = subQuery.Sink.Write(storage.PointBuffer{
				Time:   buffer.Time,
				Values: buffer.Values[i : i+len(subQuery.Columns)],
			})
			i += len(subQuery.Columns)
		}
	}
}
