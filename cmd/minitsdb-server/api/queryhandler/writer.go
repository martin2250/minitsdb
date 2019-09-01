package queryhandler

import (
	"encoding/binary"
	"encoding/json"
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"io"
	"math"
	"strconv"
	"sync"
)

type httpQueryResultWriter struct {
	Writer  io.Writer // the http response writer
	Mux     *sync.Mutex
	Index   int // the index of this subquery in the http response
	Columns []minitsdb.QueryColumn

	binary bool //todo: replace with enum
}

func (w *httpQueryResultWriter) Write(buffer storage.PointBuffer) error {
	if w.binary {
		return w.WriteBinary(buffer)
	} else {
		return w.WriteText(buffer)
	}
}

func (w *httpQueryResultWriter) WriteText(buffer storage.PointBuffer) error {
	w.Mux.Lock()
	defer w.Mux.Unlock()

	err := json.NewEncoder(w.Writer).Encode(struct {
		SeriesIndex int
		NumValues   int
		NumPoints   int
	}{
		SeriesIndex: w.Index,
		NumPoints:   buffer.Len(),
		NumValues:   buffer.Cols() - 1,
	})

	if err != nil {
		return err
	}

	for i := range buffer.Values[0] {
		line := make([]byte, 0, 100)
		for j := range buffer.Values {
			line = strconv.AppendInt(line, buffer.Values[j][i], 10)
			line = append(line, ' ')
		}
		line[len(line)-1] = '\n'
		_, err = w.Writer.Write(line)
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *httpQueryResultWriter) WriteBinary(buffer storage.PointBuffer) error {
	w.Mux.Lock()
	defer w.Mux.Unlock()

	err := json.NewEncoder(w.Writer).Encode(struct {
		SeriesIndex int
		NumValues   int
		NumPoints   int
	}{
		SeriesIndex: w.Index,
		NumPoints:   buffer.Len(),
		NumValues:   buffer.Cols() - 1,
	})

	if err != nil {
		return err
	}

	err = binary.Write(w.Writer, binary.LittleEndian, buffer.Values[0])

	for i, vals := range buffer.Values[1:] {
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
