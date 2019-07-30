package storage

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func runEncodeTest(values [][]int64) (int, error) {
	t := make([]Transformer, len(values))
	for i := range t {
		t[i] = DiffTransformer{N: 1}
	}

	var b bytes.Buffer

	n, err := EncodeBlock(values, t, &b)

	if err != nil {
		return 0, err
	}

	if b.Len() != 4096 {
		return 0, fmt.Errorf("did not write %d bytes, not 4096 bytes", b.Len())
	}

	d := NewDecoder()
	d.Columns = make([]DecoderColumn, len(t))
	for i, x := range t {
		d.Columns[i].Transformer = x
		d.Columns[i].Index = i
	}
	d.SetReader(&b)

	decoded, err := d.DecodeBlock()

	if err != nil {
		return 0, err
	}

	for i := range decoded {
		for j := range decoded[i] {
			if values[i][j] != decoded[i][j] {
				return 0, fmt.Errorf("decoded value incorrect %d (expected %d) at pos (%d, %d)", decoded[i][j], values[i][j], i, j)
			}
		}
	}

	return n, nil
}

func BenchmarkEncoder(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	values := make([][]int64, 8)

	for i := range values {
		values[i] = make([]int64, 500)
		last := rand.Int63n(600)
		for j := range values[i] {
			last = last + rand.Int63n(50) - 25
			values[i][j] = last
		}
	}

	for n := 0; n < b.N; n++ {
		j := 0
		for j < len(values[0]) {
			c, err := runEncodeTest(values[j:])
			if err != nil {
				b.Error(err)
				return
			}
			j += c
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
