package queryhandler

import (
	"errors"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"github.com/sirupsen/logrus"
	"io"
	"sync"
	"time"
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
	SubQueries []*SubQuery
	TimeStart  time.Time
}

func (c *QueryCluster) Execute() error {
	var columns []minitsdb.QueryColumn
	for _, subQuery := range c.SubQueries {
		columns = append(columns, subQuery.Columns...)
	}

	query := c.Parameters.Series.Query(columns, c.Parameters.Range, c.Parameters.TimeStep)

	defer func() {
		for _, subQuery := range c.SubQueries {
			subQuery.Done.Done()
		}

		d := time.Now().Sub(c.TimeStart)
		logrus.WithFields(logrus.Fields{"duration": d}).Info("query cluster complete")
	}()

	c.TimeStart = time.Now()

	for {
		buffer, err := query.Next()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		if buffer.Len() == 0 {
			continue
		}

		if buffer.Cols() != len(columns)+1 {
			// this should never happen, maybe replace with panic or just leave out?
			return errors.New("query returned buffer with different size")
		}

		i := 1
		for _, subQuery := range c.SubQueries {
			values := make([][]int64, len(subQuery.Columns)+1)
			// copy time
			values[0] = buffer.Values[0]
			copy(values[1:], buffer.Values[i:i+len(subQuery.Columns)])
			i += len(subQuery.Columns)

			_ = subQuery.Sink.Write(storage.PointBuffer{Values: values})
		}
	}
}
