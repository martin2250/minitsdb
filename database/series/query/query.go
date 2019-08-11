package query

import (
	"github.com/martin2250/minitsdb/database/series/storage"
	"io"
)

// Column is the combination of a column index and aggregation
type Column struct {
	Index       int
	Downsampler Downsampler
}

type Parameters struct {
	TimeStep  int64
	Columns   []Column
	TimeStart int64
	TimeEnd   int64
}

type PointSource interface {
	Next() (storage.PointBuffer, error)
}

// Query does stuff
type Query struct {
	Param Parameters
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
