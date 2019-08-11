package query

import (
	"github.com/martin2250/minitsdb/database/series/storage"
	"github.com/martin2250/minitsdb/database/series/storage/encoding"
	"github.com/martin2250/minitsdb/util"
	"io"
	"math"
)

// todo: rename this mf
// FirstPointSource reads and aggregates points from the first bucket of a series and RAM
// this is separate from highpointsource (needs renaming even more than this), as both the first bucket and ram don't have pre-downsampled values
type FirstPointSource struct {
	params       *Parameters
	buffer       storage.PointBuffer
	reader       encoding.FileDecoder
	rambuffer    storage.PointBuffer
	transformers []encoding.Transformer
}

func (s *FirstPointSource) Next() (storage.PointBuffer, error) {
	if s.params == nil {
		return storage.PointBuffer{}, io.EOF
	}

	// isEOF is set when the reader returned no new values
	// if this flag is true, the points from the ram buffer are added to the buffer
	var isEOF = false

	// read header and find first block that contains points within query range
	header := encoding.BlockHeader{
		TimeLast: math.MinInt64,
	}

	for header.TimeLast < s.params.TimeStart {
		var err error
		header, err = s.reader.DecodeHeader()

		if err == io.EOF || header.TimeFirst > s.params.TimeEnd {
			isEOF = true
			break
		} else if err != nil {
			return storage.PointBuffer{}, err
		}
	}

	// read block from reader
	if !isEOF {
		decoded, err := s.reader.DecodeBlock()

		if err == io.EOF {
			isEOF = true
		} else if err != nil {
			return storage.PointBuffer{}, err
		} else {
			// append values to buffer
			timeNew, err := encoding.TimeTransformer.Revert(decoded[0])
			if err != nil {
				return storage.PointBuffer{}, err
			}

			valuesNew := make([][]int64, len(s.params.Columns))
			for i, d := range decoded[1:] {
				valuesNew[i], err = s.transformers[i].Revert(d)
				if err != nil {
					return storage.PointBuffer{}, err
				}
			}

			s.buffer.AppendBuffer(storage.PointBuffer{
				Time:   timeNew,
				Values: valuesNew,
			})
		}
	}

	if isEOF {
		// append RAM values if bucket reader is at end
		// loop over all points in RAM
		for i := range s.rambuffer.Time {
			s.buffer.InsertPoint(s.rambuffer.At(i))
		}
	}

	// create output array
	output := storage.PointBuffer{
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
		s.params = nil
	}

	return output, nil
}

// NewFirstPointSource creates a new fps and initializes the bucket reader
// valuesRAM: points that are stored in series.Values
// params.TimeStart gets adjusted to reflect the values already read
func NewFirstPointSource(files []storage.DataFile, params *Parameters, rambuffer storage.PointBuffer, transformers []encoding.Transformer) FirstPointSource {
	// create list of relevant files
	filesRange := make([]string, 0, 8)

	for _, file := range files {
		if file.TimeEnd >= params.TimeStart || file.TimeStart >= params.TimeEnd {
			filesRange = append(filesRange, file.Path)
		}
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
		buffer:       storage.NewPointBuffer(len(params.Columns)),
		params:       params,
		reader:       encoding.NewFileDecoder(filesRange, colIndices),
		rambuffer:    rambuffer,
		transformers: transformers,
	}

	return src
}
