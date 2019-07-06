package simple8bv1

import "errors"

// value used to represent nan for unencoded data
const nanval int64 = -9223372036854775808

// value used to represent nan in encoded data (before simple8b)
const nanvalu uint64 = uint64((1 << 60) - 1)

// compute diff of array in place
func doDiff(values []int64) {
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

func undoDiff(values []int64) {
	var sum int64

	for ival, val := range values {
		if val == nanval {
			values[ival] = nanval
		} else {
			sum += val
			values[ival] = sum
		}
	}
}

// EncodeSimple8bv1 ...
func EncodeSimple8bv1(data [][]uint64) ([]byte, int, error) {

	return nil, 0, errors.New("not implemented")
}

// DecodeSimple8bv1 ...
func DecodeSimple8bv1(block []byte) ([][]uint64, error) {
	return nil, errors.New("not implemented")
}
