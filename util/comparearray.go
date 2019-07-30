package util

func Compare1DInt64(i1, i2 []int64) bool {
	if len(i1) != len(i2) {
		return false
	}
	for i := range i1 {
		if i1[i] != i2[i] {
			return false
		}
	}
	return true
}

func Compare1DUInt64(i1, i2 []uint64) bool {
	if len(i1) != len(i2) {
		return false
	}
	for i := range i1 {
		if i1[i] != i2[i] {
			return false
		}
	}
	return true
}

func Compare2DUInt64(i1, i2 [][]int64) bool {
	if len(i1) != len(i2) {
		return false
	}
	for i := range i1 {
		if len(i1[i]) != len(i2[i]) {
			return false
		}
		for j := range i1[i] {
			if i1[i][j] != i2[i][j] {
				return false
			}
		}
	}
	return true
}
