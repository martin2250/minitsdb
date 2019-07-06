package main

import (
	_ "github.com/jwilder/encoding/simple8b"
)

type shuffleVariant struct {
	Shuffle   func([]int64) []uint64
	Deshuffle func([]int64) []uint64
}

var shuffleVariants = []shuffleVariant{
	shuffleVariant{},
}

// value used to represent nan for unencoded data
const nanval int64 = -9223372036854775808

// value used to represent nan in encoded data (before simple8b)
const nanvalu uint64 = uint64((1 << 60) - 1)

// compute diff of array in place
func shuffleDiff(values []int64) {
	var last int64

	for ival, val := range values {
		if val == nanval {
			values[ival] = nanval
		} else {
			values[ival] = val - last
			last = val
		}
	}
}

// convert signed to unsigned by using the pattern 0 -1 1 -2 2 etc
// good for encoding series that contain positive and negative values
func unsignZigzag(valuesIn []int64) []uint64 {
	valuesOut := make([]uint64, len(valuesIn))

	for ival, val := range valuesIn {
		if val == nanval {
			valuesOut[ival] = nanvalu
		} else {
			valuesOut[ival] = uint64(val<<1) ^ uint64(val>>63)
		}
	}

	return valuesOut
}

// convert signed to unsigned by remapping negative numbers to numbers above (1 << 59)
// good for encoding series that only contain positive values
func unsignPositive(valuesIn []int64) []uint64 {
	valuesOut := make([]uint64, len(valuesIn))

	for ival, val := range valuesIn {
		if val >= 0 {
			valuesOut[ival] = uint64(val)
		} else {
			valuesOut[ival] = uint64((1 << 60) - 1 + val)
		}
	}

	return valuesOut
}

// make series with nanval encode better by using 0 as codeword for nan
func improveNaN(values []uint64) {
	for ival, val := range values {
		values[ival] = val + 1
	}
}
