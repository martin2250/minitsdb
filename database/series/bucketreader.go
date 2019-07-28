package series

import (
	"fmt"
	"io"
	"os"

	"github.com/martin2250/minitsdb/database/series/encoder"
	"github.com/martin2250/minitsdb/util"
)

// BucketReaderParameters holds the parameters of a query
type BucketReaderParameters struct {
	TimeFrom int64
	TimeTo   int64
	Columns  []int
}

// BucketReader is used to read data from a bucket
type BucketReader struct {
	bucket      Bucket
	fileTimes   []int64  // start times of files to be queried
	currentFile *os.File // file that is currently being read
	params      BucketReaderParameters
}

// ErrQueryEnd indicated no more values to read
var ErrQueryEnd = io.EOF // fmt.Errorf("Query has no more values")

// ErrQueryError indicated read error
var ErrQueryError = fmt.Errorf("Read error")

// ReadNextBlock reads one 4k block and returns the values as values[point][column]
// when there are no more points to read, ErrQueryEnd returned. values might still contain valid data
// subsequent reads will also return ErrQueryEnd
func (q *BucketReader) ReadNextBlock() ([][]int64, error) {
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
		valuesOut[i] = values[col][indexStart:indexEnd]
	}

	return valuesOut, err
}

func (r *BucketReader) Close() {
	r.currentFile.Close()
}

// CreateReader creates a Query on a Bucket
// from, to: time range
// columns: list of columns to return
func (b Bucket) CreateReader(params BucketReaderParameters) (r BucketReader, err error) {
	//return BucketReader{}, errors.New("this must be fixed ASAP")
	r.params = params
	r.params.TimeFrom = util.RoundDown(r.params.TimeFrom, b.TimeResolution)
	r.params.TimeTo = util.RoundDown(r.params.TimeTo, b.TimeResolution)
	r.bucket = b

	// get a list of all files in that bucket
	r.fileTimes, err = b.GetDataFiles()

	if err != nil {
		return BucketReader{}, err
	}

	// find first file that contains data points for the query
	for len(r.fileTimes) > 0 {
		// check if file start after query range
		if r.fileTimes[0] >= r.params.TimeTo {
			return BucketReader{}, ErrQueryEnd
		}

		// check if file ends before query range
		// todo: check this, not sure if logic sound
		if r.fileTimes[0] < util.RoundDown(r.params.TimeFrom, int64(b.PointsPerFile)) {
			r.fileTimes = r.fileTimes[1:]
			continue
		}

		// open database file
		r.currentFile, err = os.Open(b.GetFileName(r.fileTimes[0]))
		r.fileTimes = r.fileTimes[1:]

		if err != nil {
			return BucketReader{}, err
		}

		// find first block to contain data for the query
		var blockIndex int64

		for {
			// go to start of block
			_, err := r.currentFile.Seek(blockIndex*4096, io.SeekStart)

			if err != nil {
				r.currentFile.Close()
				return BucketReader{}, err
			}

			// decode header
			header, err := encoder.DecodeHeader(r.currentFile)

			if err != nil {
				// end of file = check next file
				if err == io.EOF {
					break
				}
				r.currentFile.Close()
				return BucketReader{}, err
			}

			// check if block starts after the query range ends
			if int64(header.TimeFirst) > r.params.TimeTo {
				return BucketReader{}, ErrQueryEnd
			}

			// check if block ends after the query range starts
			if int64(header.TimeLast) >= r.params.TimeFrom {
				_, err = r.currentFile.Seek(blockIndex*4096, io.SeekStart)

				if err != nil {
					r.currentFile.Close()
					return BucketReader{}, err
				}

				return r, nil
			}

			// check next block
			blockIndex++
		}

		r.currentFile.Close()
	}
	return r, nil
	// no files remaining
	return BucketReader{}, ErrQueryEnd
}
