package series

import (
	"fmt"
	"io"
	"os"

	"github.com/martin2250/minitsdb/encoder"
	"github.com/martin2250/minitsdb/util"
)

// Query is used to read data from a bucket
type Query struct {
	bucket      Bucket
	fileTimes   []int64  // start times of files to be queried
	currentFile *os.File // file that is currently being read
	TimeFrom    int64
	TimeTo      int64
	Columns     []int // columns to be returned
}

// ErrQueryEnd indicated no more values to read
var ErrQueryEnd = fmt.Errorf("Query has no more values")

// ErrQueryError indicated read error
var ErrQueryError = fmt.Errorf("Read error")

// ReadNextBlock reads one 4k block and returns the values as values[point][column]
// when there are no more points to read, ErrQueryEnd returned. values might still contain valid data
// subsequent reads will also return ErrQueryEnd
func (q *Query) ReadNextBlock() ([][]int64, error) {
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
		if len(q.fileTimes) == 0 || q.fileTimes[0] >= q.TimeTo {
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
		if time >= q.TimeFrom {
			indexStart = i
			break
		}
	}

	// this needs to be optimized in the future
	var indexEnd = int(header.NumPoints)

	for i, time := range values[0] {
		if time > q.TimeTo {
			indexEnd = i
			err = ErrQueryEnd
			break
		}
	}

	// create values array
	valuesOut := make([][]int64, len(q.Columns))

	// todo: check if columns are ok
	for i, col := range q.Columns {
		valuesOut[i] = values[col][indexStart:indexEnd]
	}

	return valuesOut, err
}

// CreateQuery creates a Query on a Bucket
// from, to: time range
// columns: list of columns to return
func (b Bucket) CreateQuery(from, to int64, columns []int) (q Query, err error) {
	q.TimeFrom = util.RoundDown(from, b.PointsPerFile)
	q.TimeTo = util.RoundDown(to, b.PointsPerFile)
	q.bucket = b
	q.Columns = columns

	// get a list of all files in that bucket
	q.fileTimes, err = b.GetDataFiles()

	if err != nil {
		return Query{}, err
	}

	// find first file that contains data points for the query
	for len(q.fileTimes) > 0 {
		// check if file start after query range
		if q.fileTimes[0] >= q.TimeTo {
			return Query{}, ErrQueryEnd
		}

		// check if file ends before query range
		if q.fileTimes[0] < util.RoundDown(q.TimeFrom, b.PointsPerFile) {
			q.fileTimes = q.fileTimes[1:]
			continue
		}

		// open database file
		q.currentFile, err = os.Open(b.GetFileName(q.fileTimes[0]))
		q.fileTimes = q.fileTimes[1:]

		if err != nil {
			return Query{}, err
		}

		// find first block to contain data for the query
		var blockIndex int64

		for {
			// go to start of block
			_, err := q.currentFile.Seek(blockIndex*4096, io.SeekStart)

			if err != nil {
				q.currentFile.Close()
				return Query{}, err
			}

			// decode header
			header, err := encoder.DecodeHeader(q.currentFile)

			if err != nil {
				// end of file = check next file
				if err == io.EOF {
					break
				}
				q.currentFile.Close()
				return Query{}, err
			}

			// check if block starts after the query range ends
			if int64(header.TimeFirst) > q.TimeTo {
				return Query{}, ErrQueryEnd
			}

			// check if block ends after the query range starts
			if int64(header.TimeLast) >= q.TimeFrom {
				_, err = q.currentFile.Seek(blockIndex*4096, io.SeekStart)

				if err != nil {
					q.currentFile.Close()
					return Query{}, err
				}

				return q, nil
			}

			// check next block
			blockIndex++
		}

		q.currentFile.Close()
	}

	// no files remaining
	return Query{}, ErrQueryEnd
}
