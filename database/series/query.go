package series

import (
	"github.com/martin2250/minitsdb/database/series/downsampling"
	"github.com/martin2250/minitsdb/database/series/storage"
	"io"
)

// QueryColumn is the combination of a column index and aggregation
type QueryColumn struct {
	Index       int
	Downsampler downsampling.Function
}

// Range represents a time range
type TimeRange struct {
	Start int64
	End   int64
}

func (r TimeRange) ContainsRange(other TimeRange) bool {
	return other.End <= r.End && other.Start >= r.Start
}
func (r TimeRange) Contains(time int64) bool {
	return r.Start <= time && time <= r.End
}

func (r TimeRange) Overlaps(other TimeRange) bool {
	return r.Contains(other.Start) || r.Contains(other.End) || other.ContainsRange(r)
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
