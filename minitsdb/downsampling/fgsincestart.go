package downsampling

import "errors"

type sinceStartFunction struct {
	first      int64
	populated  bool
	aggregator Aggregator
}

func (ss sinceStartFunction) Needs(indices []bool) {
	ss.aggregator.Needs(indices)
}

func (ss *sinceStartFunction) AggregatePrimary(values []int64, times []int64) int64 {
	v := ss.aggregator.AggregatePrimary(values, times)
	if !ss.populated {
		ss.first = v
		ss.populated = true
		return 0
	}
	return v - ss.first
}

func (ss *sinceStartFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	v := ss.aggregator.AggregateSecondary(values, times, counts)
	if !ss.populated {
		ss.first = v
		ss.populated = true
		return 0
	}
	return v - ss.first
}

type sinceStartFunctionGenerator struct {
}

func (sinceStartFunctionGenerator) Create(args map[string]string) (Function, error) {
	sa, ok := args["aggregator"]
	if !ok {
		return nil, errors.New("argument 'aggregator' missing")
	}

	a, ok := Aggregators[sa]
	if !ok {
		return nil, errors.New("aggregator not found")
	}

	return &sinceStartFunction{
		aggregator: a,
	}, nil
}
