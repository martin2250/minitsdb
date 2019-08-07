package encoding

const BlockSize = 4096

func applyZigzag(i int64) uint64 {
	return uint64((i >> 63) ^ (i << 1))
}

func revertZigzag(i uint64) int64 {
	return int64((i >> 1) ^ -(i & 1))
}

func applyDiff(i []int64) []int64 {
	o := make([]int64, len(i))

	var last int64

	for ival, val := range i {
		o[ival] = val - last
		last = val
	}

	return o
}

func revertDiff(i []int64) []int64 {
	o := make([]int64, len(i))

	var sum int64

	for ival, val := range i {
		sum += val
		o[ival] = sum
	}

	return o
}
