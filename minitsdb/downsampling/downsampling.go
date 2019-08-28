package downsampling

import (
	"errors"
	"strings"
)

// Function is a function of input data that can be requested by a query
type Function interface {
	// Needs sets indices to true at the index of every aggregation necessary
	// to calculate this function
	Needs(indices []bool)

	// AggregatePrimary calculates the aggregation from a primary bucket
	AggregatePrimary(values []int64, times []int64) int64

	// AggregateSecondary calculates the aggregation from a secondary bucket
	// values is an array of the data of all aggregations registered to the
	// system, input only needs to contain values at the indices indicated by
	// Needs()
	AggregateSecondary(values [][]int64, times []int64, counts []int64) int64
}

// Aggregator is an aggregator that can be stored in a bucket additional
// to being requested by a query.
// The distinction is made so less memory needs to be allocated during queries
// for functions that are never stored in a file or are not stored for every
// column individually, like e.g. count
type Aggregator interface {
	GetIndex() int

	Needs(indices []bool)
	AggregatePrimary(values []int64, times []int64) int64
	AggregateSecondary(values [][]int64, times []int64, counts []int64) int64
}

type FunctionGenerator interface {
	Create(args map[string]string) (Function, error)
}

// aggregators
var (
	First = firstAggregator{index: 0}
	Last  = lastAggregator{index: 1}
	Min   = minAggregator{index: 2}
	Max   = maxAggregator{index: 3}
	Sum   = sumAggregator{index: 4}
	Mean  = meanAggregator{index: 5}
)

var Aggregators = map[string]Aggregator{
	"first": First,
	"last":  Last,
	"min":   Min,
	"max":   Max,
	"sum":   Sum,
	"mean":  Mean,
}

var AggregatorList = []Aggregator{
	First,
	Last,
	Min,
	Max,
	Sum,
	Mean,
}

var AggregatorCount = len(Aggregators)

// functions
var (
	Count      countFunction
	Difference differenceFunction
	PeakPeak   peakpeakFunction
)

var Functions = map[string]Function{
	"count":      Count,
	"difference": Difference,
	"diff":       Difference,
	"peakpeak":   PeakPeak,
}

var FunctionCount int

// add aggregators to functions
func init() {
	for name, agg := range Aggregators {
		Functions[name] = agg
	}
	FunctionCount = len(Functions)
}

// function generators
var (
	Derivative derivativeFunctionGenerator
	Accumulate accumulateFunctionGenerator
	Integrate  integrateFunctionGenerator
	SinceStart sinceStartFunctionGenerator
)

var FunctionGenerators = map[string]FunctionGenerator{
	"derivative": Derivative,
	"accumulate": Accumulate,
	"integrate":  Integrate,
	"sincestart": SinceStart,
}

// FindFunction tries to find a matching function
// if none is found, tries to find a matching function generator
// and generate a function
func FindFunction(s string) (Function, error) {
	parts := strings.Split(s, " ")

	if len(parts) < 1 {
		return nil, errors.New("function description empty")
	}

	if f, ok := Functions[parts[0]]; ok {
		return f, nil
	}

	if fg, ok := FunctionGenerators[parts[0]]; ok {
		args := make(map[string]string)

		for _, arg := range parts[1:] {
			argparts := strings.Split(arg, ":")
			if len(argparts) != 2 {
				return nil, errors.New("function description invalid")
			}
			args[argparts[0]] = argparts[1]
		}

		return fg.Create(args)
	}

	return nil, errors.New("no matching function found")
}
