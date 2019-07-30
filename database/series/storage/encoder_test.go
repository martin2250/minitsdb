package storage

import (
	"bytes"
	"fmt"
	"github.com/martin2250/minitsdb/util"
	"math/rand"
	"testing"
	"time"
)

func runEncodeTest(values [][]uint64, times []int64) error {
	var b bytes.Buffer

	valuesC := util.Copy2DUInt64(values)
	timesC := util.Copy1DInt64(times)

	err := EncodeAll(&b, timesC, valuesC)

	if err != nil {
		return err
	}

	if !util.Compare1DInt64(times, timesC) || !util.Compare2DUInt64(values, valuesC) {
		return fmt.Errorf("encoder modified input arrays", b.Len())
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

func createData(n, N, d int) ([][]uint64, []int64) {
	values := make([][]int64, n)

	for i := range values {
		values[i] = make([]int64, N)
		last := rand.Int63n(d)
		for j := range values[i] {
			last = last + rand.Int63n(d) - int64(d/2)
			values[i][j] = last
		}
	}

	valuesE := make([][]uint64, len(values))
	for i := range values {
		var err error
		valuesE[i], err = DiffTransformer{N: 1}.Apply(values[i])
		if err != nil {
			panic(err)
		}
	}

	return valuesE, values[0]
}

func BenchmarkEncoder(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	values, time := createData(10, 1000, 500)

	b.StartTimer()
	for n := 0; n < b.N; n++ {
		err := runEncodeTest(values, time)
		if err != nil {
			b.Error(err)
			return
		}
	}
	b.StopTimer()
}

func TestEncoder(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	values, time := createData(10, 1000, 500)

	err := runEncodeTest(values, time)

	if err != nil {
		t.Error(err)
		return
	}
}
