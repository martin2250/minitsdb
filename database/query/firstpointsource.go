package query

import (
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/database/series/storage"
	"github.com/martin2250/minitsdb/util"
	"io"
	"math"
)

// todo: rename this mf
// FirstPointSource reads and aggregates points from the first bucket of a series and RAM
// this is separate from highpointsource (needs renaming even more than this), as both the first bucket and ram don't have pre-downsampled values
type FirstPointSource struct {
	series *series.Series
	params *Parameters
	buffer series.PointBuffer
	reader storage.FileDecoder
}

func (s *FirstPointSource) Next() (series.PointBuffer, error) {
	// read header and find first block that contains points within query range
	header := storage.BlockHeader{
		TimeLast: math.MinInt64,
	}

	for header.TimeLast < s.params.TimeStart {
		var err error
		header, err = s.reader.DecodeHeader()

		if err != nil {
			return series.PointBuffer{}, err
		}

		if header.TimeFirst > s.params.TimeEnd {
			return series.PointBuffer{}, io.EOF
		}
	}
	// read block from reader
	decoded, err := s.reader.DecodeBlock()

	// check for EOF seperately
	var isEOF = false

	if err == io.EOF {
		isEOF = true
	} else if err != nil {
		return series.PointBuffer{}, err
	}

	if !isEOF {
		// append values to buffer
		timeNew, err := storage.TimeTransformer.Revert(decoded[0])
		if err != nil {
			return series.PointBuffer{}, err
		}
		s.buffer.Time = append(s.buffer.Time, timeNew...)

		valuesNew := make([][]int64, len(s.params.Columns))
		for i, d := range decoded[1:] {
			valuesNew[i], err = s.series.Columns[s.params.Columns[i].Index].Transformer.Revert(d)
			if err != nil {
				return series.PointBuffer{}, err
			}
		}

		s.buffer.AppendBuffer(series.PointBuffer{
			Time:   timeNew,
			Values: valuesNew,
		})
	} else {
		// append RAM values if bucket reader is at end
		// loop over all points in RAM
		for i := range s.series.Buffer.Time {
			s.buffer.InsertPoint(s.series.Buffer.At(i))
		}
	}

	// create output array
	output := series.PointBuffer{
		Time:   make([]int64, 0),
		Values: make([][]int64, len(s.params.Columns)),
	}

	for i := range output.Values {
		output.Values[i] = make([]int64, 0)
	}

	// downsample data into output array
	// todo: continue work here
	for s.buffer.Len() > 0 {
		var timeStepStart = util.RoundDown(s.buffer.Time[0], s.params.TimeStep)

		// stop if this time step exceeds timeend
		if timeStepStart > s.params.TimeEnd {
			isEOF = true
			s.reader.Close()
			break
		}

		// find first point that doesn't belong in this time step anymore
		var indexEnd = -1
		for i, timeend := range s.buffer.Time {
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
				indexEnd = len(s.buffer.Time)
			} else {
				break
			}
		}

		// append time of point to output
		output.Time = append(output.Time, timeStepStart)
		// append downsampled values
		for indexCol, col := range s.params.Columns {
			output.Values[indexCol] = append(output.Values[indexCol], col.Downsampler.DownsampleFirst(s.buffer.Values[indexCol][0:indexEnd]))
		}

		// update query range
		s.params.TimeStart = timeStepStart + s.params.TimeStep - 1

		// stop if this time step is the last in this query
		if timeStepStart+s.params.TimeStep > s.params.TimeEnd {
			isEOF = true
			s.reader.Close()
			break
		}

		// remove points from buffer
		s.buffer.Discard(indexEnd)
	}

	if isEOF {
		return output, io.EOF
	} else {
		return output, nil
	}
}

// NewFirstPointSource creates a new fps and initializes the bucket reader
// valuesRAM: points that are stored in series.Values
// params.TimeStart gets adjusted to reflect the values already read
func NewFirstPointSource(s *series.Series, params *Parameters) FirstPointSource {
	b := s.Buckets[0]
	// create list of relevant files in first bucket
	files := make([]string, 0, len(b.DataFiles))

	for time, file := range b.DataFiles {
		// check file ends before query start
		if time+b.TimeResolution*(int64(b.PointsPerFile)-1) < params.TimeStart {
			continue
		}
		// check if file begins after query end
		if time > params.TimeEnd {
			continue
		}
		files = append(files, file.Path)
	}

	// create column indices for decoder
	colIndices := make([]int, len(params.Columns)+1)
	colIndices[0] = 0 // time
	for i, col := range params.Columns {
		colIndices[i+1] = col.Index + 1
	}

	// fix parameter times to make further roundin operations unnecessary
	// todo: this should probably be somewhere else
	// we only want values where roundDown(time) >= timeStart
	params.TimeStart = util.RoundUp(params.TimeStart, params.TimeStep)
	// we need values after timeEnd, as roundDown(time) might still be <= timeEnd
	params.TimeEnd = util.RoundDown(params.TimeEnd, params.TimeStep) + params.TimeStep - 1

	// create point source struct
	src := FirstPointSource{
		series: s,
		buffer: series.NewPointBuffer(len(params.Columns)),
		params: params,
		reader: storage.NewFileDecoder(files, colIndices),
	}

	return src
}
