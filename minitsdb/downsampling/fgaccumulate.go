package downsampling

import (
	"errors"
)

type accumulateFunction struct {
	accumulator int64
	aggregator  Aggregator
}

func (af accumulateFunction) Needs(indices []bool) {
	af.aggregator.Needs(indices)
}

func (af *accumulateFunction) AggregatePrimary(values []int64, times []int64) int64 {
	af.accumulator += af.aggregator.AggregatePrimary(values, times)
	return af.accumulator
}

func (af *accumulateFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	af.accumulator += af.aggregator.AggregateSecondary(values, times, counts)
	return af.accumulator
}

type accumulateFunctionGenerator struct {
}

func (accumulateFunctionGenerator) Create(args map[string]string) (Function, error) {
	sa, ok := args["aggregator"]
	if !ok {
		return nil, errors.New("argument 'aggregator' missing")
	}

	a, ok := Aggregators[sa]
	if !ok {
		return nil, errors.New("aggregator not found")
	}

	return &accumulateFunction{
		aggregator: a,
	}, nil
}
