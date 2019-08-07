package encoding

import (
	"bytes"
	"github.com/martin2250/minitsdb/util"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"
)

func runEncodeTest(values [][]uint64, times []int64) error {

	return nil
}

func createData(n, N, d int) ([][]uint64, []int64) {
	values := make([][]int64, n)

	for i := range values {
		values[i] = make([]int64, N)
		last := rand.Int63n(int64(d))
		for j := range values[i] {
			last = last + rand.Int63n(int64(d)) - int64(d/2)
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

	values, time := createData(10, b.N, 500)

	b.StartTimer()

	err := EncodeAll(ioutil.Discard, time, values)
	if err != nil {
		b.Error(err)
		return
	}

	b.StopTimer()
}

func BenchmarkDecoder(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	values, time := createData(10, b.N, 500)

	var buffer bytes.Buffer

	err := EncodeAll(&buffer, time, values)
	if err != nil {
		b.Error(err)
		return
	}

	d := NewDecoder()
	d.Columns = make([]int, len(values))
	for i := range d.Columns {
		d.Columns[i] = i
	}
	d.SetReader(&buffer)

	b.StartTimer()

	var n int
	for n < len(time) {
		decoded, err := d.DecodeBlock()

		if err != nil {
			b.Error(err)
			return
		}

		n += len(decoded[0])
	}

	b.StopTimer()
}

func TestEncoder(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	values, times := createData(10, 1000, 500)

	var b bytes.Buffer

	valuesC := util.Copy2DUInt64(values)
	timesC := util.Copy1DInt64(times)

	err := EncodeAll(&b, timesC, valuesC)

	if err != nil {
		t.Error(err)
		return
	}

	if !util.Compare1DInt64(times, timesC) || !util.Compare2DUInt64(values, valuesC) {
		t.Error("encoder modified input arrays")
		return
	}

	if b.Len()%4096 != 0 {
		t.Errorf("encoder wrote %d bytes, not a multiple of 4096 bytes", b.Len())
		return
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
			t.Error(err)
			return
		}

		for i := range decoded {
			for j := range decoded[i] {
				if values[i][n+j] != decoded[i][j] {
					t.Errorf("decoded value incorrect %d (expected %d) at pos (%d, %d)", decoded[i][j], values[i][n+j], i, n+j)
					return
				}
			}
		}

		n += len(decoded[0])
	}
}
