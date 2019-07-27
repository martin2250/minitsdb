package query

import (
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/util"
)

// Column is the combination of a column index and aggregation
type Column struct {
	Index int
	//	Type  Aggregation
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

type Query struct {
	series *series.Series
	// remaining buckets to be read (all buckets with smaller timesteps than the query, sorted by timestep descending)
	sources []PointSource

	param Parameters
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

func NewQuery(s *series.Series, p Parameters) (Query, error) {
	// adjust time step
	p.TimeStep = findTimeStep(p.TimeStep, s.Buckets)

	// create query object
	q := Query{
		series:  s,
		sources: make([]PointSource, 0),
		param:   p,
	}

	// create

	return q, nil
}

func (q *Query) ReadNext() ([][]int64, error) {
	return nil, nil
}
