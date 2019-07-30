package util

func Copy1DInt64(i []int64) []int64 {
	o := make([]int64, len(i))
	copy(o, i)
	return o
}

func Copy1DUInt64(i []uint64) []uint64 {
	o := make([]uint64, len(i))
	copy(o, i)
	return o
}

func Copy2DInt64(i [][]int64) [][]int64 {
	o := make([][]int64, len(i))
	for j := range i {
		o[j] = make([]int64, len(i[j]))
		copy(o[j], i[j])
	}
	return o
}

func Copy2DUInt64(i [][]uint64) [][]uint64 {
	o := make([][]uint64, len(i))
	for j := range i {
		o[j] = make([]uint64, len(i[j]))
		copy(o[j], i[j])
	}
	return o
}
