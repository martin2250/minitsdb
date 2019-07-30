package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func runEncodeTest(values [][]uint64, times []int64) error {
	var b bytes.Buffer

	// todo: check if EncodeBlock modifies the input arrays
	err := EncodeAll(&b, times, values)

	if err != nil {
		return err
	}

	if b.Len()%4096 != 0 {
		return fmt.Errorf("encoder wrote %d bytes, not a multiple of 4096 bytes", b.Len())
	}

	d := NewDecoder()
	d.Columns = make([]int, len(values))
	for i := range d.Columns {
		d.Columns[i] = i
	}
	d.SetReader(&b)

	n := 0

	for n < len(times) {
		decoded, err := d.DecodeBlock()

		if err != nil {
			return err
		}

		for i := range decoded {
			for j := range decoded[i] {
				if values[i][n+j] != decoded[i][j] {
					return fmt.Errorf("decoded value incorrect %d (expected %d) at pos (%d, %d)", decoded[i][j], values[i][n+j], i, n+j)
				}
			}
		}

		n += len(decoded[0])
	}

	return nil
}

func BenchmarkEncoder(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	values := make([][]int64, 8)

	for i := range values {
		values[i] = make([]int64, 2000)
		last := rand.Int63n(600)
		for j := range values[i] {
			last = last + rand.Int63n(50) - 25
			values[i][j] = last
		}
	}

	valuesE := make([][]uint64, len(values))
	for i := range values {
		var err error
		valuesE[i], err = DiffTransformer{N: 1}.Apply(values[i])
		if err != nil {
			b.Errorf("transformer encountered error: %v", err)
			return
		}
	}
	b.StartTimer()
	for n := 0; n < b.N; n++ {
		err := runEncodeTest(valuesE, values[0])
		if err != nil {
			b.Error(err)
			return
		}
	}
	b.StopTimer()
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

	valuesE := make([][]uint64, len(values))
	for i := range values {
		var err error
		valuesE[i], err = DiffTransformer{N: 1}.Apply(values[i])
		if err != nil {
			t.Errorf("transformer encountered error: %v", err)
			return
		}
	}

	err := runEncodeTest(valuesE, values[0])

	if err != nil {
		t.Error(err)
		return
	}
}
