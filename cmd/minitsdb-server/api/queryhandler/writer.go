package queryhandler

import (
	"encoding/binary"
	"encoding/json"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"io"
	"math"
	"sync"
)

type httpQueryResultWriter struct {
	Writer  io.Writer // the http response writer
	Mux     *sync.Mutex
	Index   int // the index of this subquery in the http response
	Columns []minitsdb.QueryColumn
}

func (w *httpQueryResultWriter) Write(buffer storage.PointBuffer) error {
	w.Mux.Lock()
	defer w.Mux.Unlock()

	err := json.NewEncoder(w.Writer).Encode(struct {
		SeriesIndex int
		NumValues   int
		NumPoints   int
	}{
		SeriesIndex: w.Index,
		NumPoints:   buffer.Len(),
		NumValues:   buffer.Cols(),
	})

	if err != nil {
		return err
	}

	err = binary.Write(w.Writer, binary.LittleEndian, buffer.Time)

	for i, vals := range buffer.Values {
		fac := math.Pow10(-w.Columns[i].Column.Decimals)
		valuesf := make([]float64, len(vals))
		for j := range valuesf {
			valuesf[j] = float64(vals[j]) * fac
		}
		err = binary.Write(w.Writer, binary.LittleEndian, valuesf)
		if err != nil {
			return err
		}
	}

	return nil
}
