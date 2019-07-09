package encoder

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func runEncodeTest(values [][]int64) (int, error) {
	buffer, _, err := EncodeBlock(values)

	if err != nil {
		return 0, err
	}

	header, decoded, err := DecodeBlock(&buffer, nil)

	if err != nil {
		return 0, err
	}

	for i := range decoded {
		for j := range decoded[i] {
			if values[i][j] != decoded[i][j] {
				return 0, fmt.Errorf("Decoded value incorrect %d (expected %d) at pos (%d, %d)", decoded[i][j], values[i][j], i, j)
			}
		}
	}

	return int(header.NumPoints), nil
}

func BenchmarkEncoder(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	values := make([][]int64, 8)

	for i := range values {
		values[i] = make([]int64, 200)

		for j := range values[i] {
			values[i][j] = rand.Int63n(2525323)
		}
	}

	for n := 0; n < b.N; n++ {
		_, err := runEncodeTest(values)

		if err != nil {
			b.Error(err)
			return
		}
	}
}

func TestEncoder(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	values := make([][]int64, 8)

	for i := range values {
		values[i] = make([]int64, 200)

		for j := range values[i] {
			values[i][j] = rand.Int63n(2525323)
		}
	}

	_, err := runEncodeTest(values)

	if err != nil {
		t.Error(err)
		return
	}
}
