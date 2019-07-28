package query

import (
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/database/series"
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
	Next() ([][]int64, error)
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

func findTimeStep(queryStep int64, buckets []series.Bucket) int64 {
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

func (q *Query) ReadNext() ([][]int64, error) {
	var values [][]int64
	var err error

	for values == nil {
		if q.currentSource == nil {
			if len(q.buckets) == 0 {
				return nil, io.EOF
			}
			b := q.series.Buckets[q.buckets[0]]
			if b.First {
				source, err := NewFirstPointSource(b, q.series.Values, &q.Param)

				if err != nil {
					return nil, err
				}

				q.currentSource = &source
			} else {
				return nil, errors.New("highpointsource not implemented yet")
			}
		}

		values, err = q.currentSource.Next()

		if err != nil {
			q.currentSource = nil
			if err != io.EOF {
				return nil, err
			}
		}
	}

	return values, err
}
