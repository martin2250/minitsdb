package query

import (
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/database/series/storage"
	"github.com/martin2250/minitsdb/util"
	"io"
)

// Column is the combination of a column index and aggregation
type Column struct {
	Index       int
	Downsampler series.Downsampler
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
	series *series.Series
	// remaining buckets to be read (all buckets with smaller timesteps than the query, sorted by timestep descending)
	buckets []int

	Param Parameters

	currentSource PointSource

	Writer io.Writer

	Done chan struct{}
}

func findTimeStep(queryStep int64, buckets []storage.Bucket) int64 {
	// find largest bucketStep smaller than queryStep
	var largestBucketStep int64 = 0

	for _, bucket := range buckets {
		if bucket.TimeResolution <= queryStep {
			largestBucketStep = bucket.TimeResolution
		}
	}

	// no bucket is fine enough to satisfy query, choose smallest bucket
	if largestBucketStep == 0 {
		return buckets[0].TimeResolution
	}

	// round up query step to next multiple of largestBucketStep
	return util.RoundUp(queryStep, largestBucketStep)
}

// NewQuery creates a query object on s with parameters p
func NewQuery(s *series.Series, p Parameters, w io.Writer) Query {
	// adjust time step (not dependent on data, done before init function)
	p.TimeStep = findTimeStep(p.TimeStep, s.Buckets)

	// create query object
	q := Query{
		series:  s,
		buckets: make([]int, 0),
		Param:   p,
		Writer:  w,
	}

	// add all buckets that have time step small enough
	// order is reversed (coarse buckets first)
	for i, b := range s.Buckets {
		if p.TimeStep >= b.TimeResolution {
			q.buckets = append([]int{i}, q.buckets...)
		}
	}

	// this should never occur, so panic away
	// todo: replace this with normal error when testing is done
	if len(q.buckets) == 0 {
		panic(fmt.Sprintf("didn't find any buckets for query, timestep not determined correctly\n%+v", q))
	}

	return q
}

func (q *Query) ReadNext() (storage.PointBuffer, error) {
	for {
		if q.currentSource == nil {
			if len(q.buckets) == 0 {
				return storage.PointBuffer{}, io.EOF
			}
			b := q.series.Buckets[q.buckets[0]]
			if b.First {
				source := NewFirstPointSource(q.series, &q.Param)
				q.currentSource = &source
			} else {
				return storage.PointBuffer{}, errors.New("highpointsource not implemented yet")
			}
		}

		buffer, err := q.currentSource.Next()

		if err == nil {
			return buffer, nil
		} else {
			q.currentSource = nil
			if err != io.EOF {
				return storage.PointBuffer{}, err
			}
		}
	}
}
