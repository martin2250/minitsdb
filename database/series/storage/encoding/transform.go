package encoding

import (
	"fmt"
)

// a transformer encodes a list of signed integer values to unsigned values which can be fed into simple8b
type Transformer interface {
	Apply(input []int64) ([]uint64, error)
	Revert(input []uint64) ([]int64, error)
}

// DiffTransformer differentiates the input data N times and uses zig-zag encoding to get rid of sign
type DiffTransformer struct {
	N int
}

var TimeTransformer = DiffTransformer{N: 2}

func (t DiffTransformer) Apply(input []int64) ([]uint64, error) {
	// make a copy of input array, else input gets modified in the calling method
	d := make([]int64, len(input))
	copy(d, input)

	// if input length is smaller than N, reduce number of diffs
	N := t.N
	if len(input) <= N {
		N = len(input) - 1
	}

	// apply diff N times
	for n := 0; n < N; n++ {
		d = append(d[:n], applyDiff(d[n:])...)
	}

	// apply zig zag encoding
	o := make([]uint64, len(input))

	for i, v := range d {
		o[i] = applyZigzag(v)
	}

	return o, nil
}

func (t DiffTransformer) Revert(input []uint64) ([]int64, error) {
	// revert zig zag encoding
	d := make([]int64, len(input))

	for i, v := range input {
		d[i] = revertZigzag(v)
	}

	// if input length is smaller than N, reduce number of diffs
	N := t.N
	if len(input) <= N {
		N = len(input) - 1
	}

	// revert diff N times
	for n := N - 1; n >= 0; n-- {
		d = append(d[:n], revertDiff(d[n:])...)
	}

	return d, nil
}

func FindTransformer(s string) (Transformer, error) {
	var arg int
	if _, err := fmt.Sscanf(s, "D%d", &arg); err != nil {
		if arg < 0 || arg > 3 {
			return nil, fmt.Errorf("%d is outside the allowed range for a diff transformer", arg)
		}
		return DiffTransformer{N: arg}, nil
	} else {
		return nil, fmt.Errorf("%s matches no known transformers", s)
	}
}
