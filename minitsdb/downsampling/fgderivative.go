package downsampling

import (
	"errors"
	"math"
	"strconv"
)

type derivativeFunction struct {
	seconds int64
	dVal    differenceFunction
	dTime   differenceFunction
}

func (d *derivativeFunction) Needs(indices []bool) {
	d.dVal.Needs(indices)
}

func (d *derivativeFunction) AggregatePrimary(values []int64, times []int64) int64 {
	diff := d.dVal.AggregatePrimary(values, nil)
	time := d.dTime.AggregatePrimary(times, nil)
	if time < 1 {
		time = 1
	}
	return (d.seconds * diff) / time
}

func (d derivativeFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	diff := d.dVal.AggregateSecondary(values, nil, nil)
	time := d.dTime.AggregatePrimary(times, nil)
	if time < 1 {
		time = 1
	}
	return (d.seconds * diff) / time
}

type derivativeFunctionGenerator struct {
}

func (derivativeFunctionGenerator) Create(args map[string]string) (Function, error) {
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

	return &derivativeFunction{
		seconds: i,
		dVal:    differenceFunction{last: math.MinInt64},
		dTime:   differenceFunction{last: math.MinInt64},
	}, nil
}
