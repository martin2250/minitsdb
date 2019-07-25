package series

import (
	"fmt"
	"io"
	"os"

	"github.com/martin2250/minitsdb/encoder"
	"github.com/martin2250/minitsdb/util"
)

// Aggregation selects how to merge multiple values into a query point
type Aggregation int

const (
	// Mean calulates the arithmetic mean of all values
	Mean Aggregation = iota
	// Min selects the smallest of all values
	Min
	// Max selects the largest of all values
	Max
)

// QueryColumn is the combination of a column and aggregation
type QueryColumn struct {
	Column int
	Type   Aggregation
}

// QueryParameters holds the parameters of a query
type QueryParameters struct {
	TimeFrom int64
	TimeTo   int64
	Columns  []QueryColumn
}

// QueryState is used to read data from a bucket
type QueryState struct {
	bucket      Bucket
	fileTimes   []int64  // start times of files to be queried
	currentFile *os.File // file that is currently being read
	params      QueryParameters
}

// ErrQueryEnd indicated no more values to read
var ErrQueryEnd = fmt.Errorf("Query has no more values")

// ErrQueryError indicated read error
var ErrQueryError = fmt.Errorf("Read error")

// ReadNextBlock reads one 4k block and returns the values as values[point][column]
// when there are no more points to read, ErrQueryEnd returned. values might still contain valid data
// subsequent reads will also return ErrQueryEnd
func (q *QueryState) ReadNextBlock() ([][]int64, error) {
	// check if is query exhausted
	if q.currentFile == nil {
		return nil, ErrQueryEnd
	}

	// decode block
	header, values, err := encoder.ReadBlock(q.currentFile)

	// file at end
	if err == io.EOF {
		q.currentFile.Close()
		q.currentFile = nil

		// check next file
		if len(q.fileTimes) == 0 || q.fileTimes[0] >= q.params.TimeTo {
			return nil, ErrQueryEnd
		}

		// open next database file
		q.currentFile, err = os.Open(q.bucket.GetFileName(q.fileTimes[0]))
		q.fileTimes = q.fileTimes[1:]

		if err != nil {
			return nil, err
		}

		// actually return a block
		// todo: make this not recursive
		return q.ReadNextBlock()
	} else if err != nil {
		return nil, err
	}

	// todo: check header for time

	// find first and last point in the query range
	var indexStart = int(header.NumPoints)

	for i, time := range values[0] {
		if time >= q.params.TimeFrom {
			indexStart = i
			break
		}
	}

	// this needs to be optimized in the future
	var indexEnd = int(header.NumPoints)

	for i, time := range values[0] {
		if time > q.params.TimeTo {
			indexEnd = i
			err = ErrQueryEnd
			break
		}
	}

	// create values array
	valuesOut := make([][]int64, len(q.params.Columns))

	// todo: check if columns are ok
	for i, col := range q.params.Columns {
		valuesOut[i] = values[col.Column][indexStart:indexEnd]
	}

	return valuesOut, err
}

// CreateQuery creates a Query on a Bucket
// from, to: time range
// columns: list of columns to return
func (b Bucket) CreateQuery(params QueryParameters) (q QueryState, err error) {
	q.params = params
	q.params.TimeFrom = util.RoundDown(q.params.TimeFrom, b.PointsPerFile)
	q.params.TimeTo = util.RoundDown(q.params.TimeTo, b.PointsPerFile)
	q.bucket = b

	// get a list of all files in that bucket
	q.fileTimes, err = b.GetDataFiles()

	if err != nil {
		return QueryState{}, err
	}

	// find first file that contains data points for the query
	for len(q.fileTimes) > 0 {
		// check if file start after query range
		if q.fileTimes[0] >= q.params.TimeTo {
			return QueryState{}, ErrQueryEnd
		}

		// check if file ends before query range
		if q.fileTimes[0] < util.RoundDown(q.params.TimeFrom, b.PointsPerFile) {
			q.fileTimes = q.fileTimes[1:]
			continue
		}

		// open database file
		q.currentFile, err = os.Open(b.GetFileName(q.fileTimes[0]))
		q.fileTimes = q.fileTimes[1:]

		if err != nil {
			return QueryState{}, err
		}

		// find first block to contain data for the query
		var blockIndex int64

		for {
			// go to start of block
			_, err := q.currentFile.Seek(blockIndex*4096, io.SeekStart)

			if err != nil {
				q.currentFile.Close()
				return QueryState{}, err
			}

			// decode header
			header, err := encoder.DecodeHeader(q.currentFile)

			if err != nil {
				// end of file = check next file
				if err == io.EOF {
					break
				}
				q.currentFile.Close()
				return QueryState{}, err
			}

			// check if block starts after the query range ends
			if int64(header.TimeFirst) > q.params.TimeTo {
				return QueryState{}, ErrQueryEnd
			}

			// check if block ends after the query range starts
			if int64(header.TimeLast) >= q.params.TimeFrom {
				_, err = q.currentFile.Seek(blockIndex*4096, io.SeekStart)

				if err != nil {
					q.currentFile.Close()
					return QueryState{}, err
				}

				return q, nil
			}

			// check next block
			blockIndex++
		}

		q.currentFile.Close()
	}

	// no files remaining
	return QueryState{}, ErrQueryEnd
}
