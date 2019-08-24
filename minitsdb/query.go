package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"io"
)

// QueryColumn is the combination of a column index and aggregation
type QueryColumn struct {
	Index       int
	Downsampler downsampling.Function
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
	Sources []PointSource
}

func (q *Query) ReadNext() (storage.PointBuffer, error) {
	for {
		if len(q.Sources) == 0 {
			return storage.PointBuffer{}, io.EOF
		}

		buffer, err := q.Sources[0].Next()

		if err == nil {
			return buffer, nil
		} else {
			q.Sources = q.Sources[1:]
			if err != io.EOF {
				return storage.PointBuffer{}, err
			}
		}
	}
}
