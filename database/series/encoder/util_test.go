package encoder

import (
	"math"
	"testing"
)

func TestTransform(t *testing.T) {
	values := []int64{
		math.MinInt64,
		math.MinInt64 + 1,
		math.MinInt64 + 2,
		0, 1, 2, 5, 36, 1, 15, -356, math.MaxInt64,
	}

	encoded := doTransform(values)

	decoded := undoTransform(encoded)

	for i := range values {
		if values[i] != decoded[i] {
			t.Errorf("decoded array does not match original %v %v", values, decoded)
			return
		}
	}
}
