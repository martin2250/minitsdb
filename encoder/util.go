package encoder

func doZigzag(i int64) uint64 {
	return uint64((i >> 63) ^ (i << 1))
}

func undoZigzag(i uint64) int64 {
	return int64((i >> 1) ^ -(i & 1))
}

func doTransform(values []int64) []uint64 {
	output := make([]uint64, len(values))

	var last int64

	for ival, val := range values {
		output[ival] = doZigzag(val - last)
		last = val
	}

	return output
}

func undoTransform(values []uint64) []int64 {
	output := make([]int64, len(values))

	var sum int64

	for ival, val := range values {
		sum += undoZigzag(val)
		output[ival] = sum
	}

	return output
}
