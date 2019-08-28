package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"io"
)

// QueryColumn is the combination of a column index and aggregation
type QueryColumn struct {
	Column   *Column
	Function downsampling.Function
}

type Parameters struct {
	TimeStep int64
	Columns  []QueryColumn
}

type PointSource interface {
	Next() (storage.PointBuffer, error)
}

// Query does stuff
type Query struct {
	Param     Parameters
	TimeRange TimeRange
	// todo: change this, this should not be public
	Sources []BucketQuerySource
}

func (q *Query) ReadNext() (storage.PointBuffer, error) {
	for {
		if len(q.Sources) == 0 {
			return storage.PointBuffer{}, io.EOF
		}

		i := len(q.Sources) - 1

		buffer, err := q.Sources[i].Next()

		if err == nil {
			return buffer, nil
		} else {
			if err != io.EOF {
				return storage.PointBuffer{}, err
			}
			q.Sources = q.Sources[:i]
		}
	}
}
