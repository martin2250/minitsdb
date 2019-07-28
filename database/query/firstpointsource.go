package query

import (
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/util"
	"io"
)

// todo: rename this mf
// FirstPointSource reads and aggregates points from the first bucket of a series and RAM
type FirstPointSource struct {
	//series      *series.Series
	params    *Parameters
	values    [][]int64
	valuesRAM [][]int64
	reader    series.BucketReader
	//currentTime int64
}

func (s *FirstPointSource) Next() ([][]int64, error) {
	// read block from reader
	valuesNext, err := s.reader.ReadNextBlock()

	// check for EOF seperately
	var isEOF = false

	if err == io.EOF {
		isEOF = true
	} else if err != nil {
		return nil, err
	}

	// append values to buffer todo: make this nil check not necessary
	if valuesNext != nil {
		for i, v := range valuesNext {
			s.values[i] = append(s.values[i], v...)
		}
	}

	// append RAM values if bucket reader is at end
	if isEOF {
		// loop over all points in RAM
		for indexRAM, time := range s.valuesRAM[0] {
			// check if the time of this point was already read by the bucketreader
			indexBuffer := util.IndexOfInt64(s.values[0], time)

			if indexBuffer == -1 {
				// time not present in buffer, append to buffer
				//for indexColumn, val := range s.valuesRAM[indexRAM] {
				//	s.values[indexColumn] = append(s.values[indexColumn], val)
				//}
				for iOut, col := range s.params.Columns {
					s.values[iOut+1] = append(s.values[iOut+1], s.valuesRAM[col.Index+1][indexRAM])
				}
				s.values[0] = append(s.values[0], time)
			} else {
				// time was already present in file, update values just in case
				for indexColumn, val := range s.valuesRAM[indexRAM] {
					s.values[indexColumn][indexBuffer] = val
				}
			}
		}
	}

	// create output array
	output := make([][]int64, len(s.params.Columns)+1)

	for i := range output {
		output[i] = make([]int64, 0)
	}

	// downsample data into output array
	for len(s.values[0]) > 0 {
		// for convenience
		time := s.values[0]

		var timeStepStart = util.RoundDown(time[0], s.params.TimeStep)

		// stop if this time step exceeds timeend
		if timeStepStart > s.params.TimeEnd {
			isEOF = true
			s.reader.Close()
			break
		}

		// find first point that doesn't belong in this time step anymore
		var indexEnd = -1
		for i, timeend := range time {
			// enough values if this point is the last of this step todo: replace -1 with bucket time step
			if timeend == timeStepStart+s.params.TimeStep-1 {
				indexEnd = i + 1
				break
			}
			// enough values if the next value does not belong in this step anymore
			if timeStepStart != util.RoundDown(timeend, s.params.TimeStep) {
				indexEnd = i
				break
			}
		}

		// not enough values to fill this time step
		if indexEnd == -1 {
			// this is firstpointsource, so there are no more points to complete this time step, just use all available values
			if isEOF {
				indexEnd = len(time)
			} else {
				break
			}
		}

		// append time of point to output
		output[0] = append(output[0], timeStepStart)
		// append downsampled values
		for indexCol, col := range s.params.Columns {
			output[indexCol+1] = append(output[indexCol+1], col.Downsampler.DownsampleFirst(s.values[indexCol+1][0:indexEnd]))
		}

		// update query range
		s.params.TimeStart = timeStepStart + s.params.TimeStep - 1

		// stop if this time step is the last in this query
		if timeStepStart+s.params.TimeStep > s.params.TimeEnd {
			isEOF = true
			s.reader.Close()
			break
		}

		// remove points from s.values
		for i := range s.values {
			s.values[i] = s.values[i][indexEnd:]
		}
	}

	if isEOF {
		return output, io.EOF
	} else {
		return output, nil
	}
}

// NewFirstPointSource creates a new fps and initializes the bucket reader
// params.TimeStart gets adjusted to reflect the values already read
func NewFirstPointSource(bucket series.Bucket, valuesRAM [][]int64, params *Parameters) (FirstPointSource, error) {
	// create point source struct
	s := FirstPointSource{
		valuesRAM: valuesRAM,
		values:    make([][]int64, len(params.Columns)+1),
		params:    params,
	}

	// init value buffer
	for i := range s.values {
		s.values[i] = make([]int64, 0)
	}

	// create bucket reader parameters
	var readerParams = series.BucketReaderParameters{
		TimeFrom: util.RoundDown(params.TimeStart, params.TimeStep),
		TimeTo:   util.RoundUp(params.TimeEnd, params.TimeStep),
		Columns:  []int{0}, // time column is always read
	}

	// copy columns
	for _, col := range params.Columns {
		readerParams.Columns = append(readerParams.Columns, col.Index+1) // offset because of time column
	}

	// create bucket reader
	var err error
	s.reader, err = bucket.CreateReader(readerParams)

	if err != nil {
		return FirstPointSource{}, err
	}

	return s, nil
}
