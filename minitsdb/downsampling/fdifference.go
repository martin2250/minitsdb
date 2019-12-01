package downsampling

import "math"

type differenceFunction struct {
	last int64
}

func (differenceFunction) Needs(indices []bool) {
	Last.Needs(indices)
	First.Needs(indices) // for compatibility reasons
}

func (d *differenceFunction) AggregatePrimary(values []int64, times []int64) int64 {
	val := Last.AggregatePrimary(values, nil)
	last := d.last
	d.last = val
	if last == math.MinInt64 {
		last = First.AggregatePrimary(values, nil)
	}
	return val - last
}

func (d *differenceFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	val := Last.AggregateSecondary(values, nil, nil)
	last := d.last
	d.last = val
	if last == math.MinInt64 {
		last = First.AggregateSecondary(values, nil, nil)
	}
	return val - last
}

type differenceFunctionGenerator struct {
}

func (differenceFunctionGenerator) Create(args map[string]string) (Function, error) {
	return &differenceFunction{
		last: math.MinInt64,
	}, nil
}
