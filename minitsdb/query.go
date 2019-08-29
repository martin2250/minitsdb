package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"github.com/sirupsen/logrus"
	"io"
	"time"
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

	Trace struct {
		Init      bool
		Points    int
		TimeStart time.Time
	}
}

func (q *Query) ReadNext() (storage.PointBuffer, error) {
	if !q.Trace.Init {
		q.Trace.TimeStart = time.Now()
		q.Trace.Init = true
	}

	for {
		if len(q.Sources) == 0 {
			return storage.PointBuffer{}, io.EOF
		}

		i := len(q.Sources) - 1

		buffer, err := q.Sources[i].Next()

		if err == nil {
			q.Trace.Points += buffer.Len()
			return buffer, nil
		} else {
			if err != io.EOF {
				return storage.PointBuffer{}, err
			}
			q.Sources = q.Sources[:i]

			logrus.WithFields(logrus.Fields{
				"points":   q.Trace.Points,
				"duration": time.Now().Sub(q.Trace.TimeStart),
			}).Info("query source exhausted")

			q.Trace.Points = 0
			q.Trace.TimeStart = time.Now()
		}
	}
}
