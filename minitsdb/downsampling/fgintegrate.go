package downsampling

import (
	"errors"
	"strconv"
)

type integrateFunction struct {
	accumulator int64
	aggregator  Aggregator
	seconds     int64
}

func (af integrateFunction) Needs(indices []bool) {
	af.aggregator.Needs(indices)
}

func (af *integrateFunction) AggregatePrimary(values []int64, times []int64) int64 {
	d := af.aggregator.AggregatePrimary(values, times)
	d *= Difference.AggregatePrimary(times, nil)
	af.accumulator += d
	return af.accumulator / af.seconds
}

func (af *integrateFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	d := af.aggregator.AggregateSecondary(values, times, counts)
	d *= Difference.AggregatePrimary(times, nil)
	af.accumulator += d
	return af.accumulator / af.seconds
}

type integrateFunctionGenerator struct {
}

func (integrateFunctionGenerator) Create(args map[string]string) (Function, error) {
	sa, ok := args["aggregator"]
	if !ok {
		return nil, errors.New("argument 'aggregator' missing")
	}

	a, ok := Aggregators[sa]
	if !ok {
		return nil, errors.New("aggregator not found")
	}

	s, ok := args["seconds"]

	if !ok {
		return nil, errors.New("argument 'seconds' missing")
	}

	i, err := strconv.ParseInt(s, 10, 64)

	if err != nil {
		return nil, err
	}

	if i < 1 {
		return nil, errors.New("argument 'seconds' must be greater than zero")
	}

	return &integrateFunction{
		aggregator: a,
		seconds:    i,
	}, nil
}
