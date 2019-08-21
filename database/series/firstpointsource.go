package series

import (
	"github.com/martin2250/minitsdb/database/series/downsampling"
	"github.com/martin2250/minitsdb/database/series/storage"
	"github.com/martin2250/minitsdb/database/series/storage/encoding"
	"github.com/martin2250/minitsdb/util"
	"io"
	"math"
	"sort"
)

// todo: rename this mf
// FirstPointSource reads and aggregates points from the first bucket of a series and RAM
// this is separate from highpointsource (needs renaming even more than this), as both the first bucket and ram don't have pre-downsampled values
type FirstPointSource struct {
	timeRange     *TimeRange
	timeStep      int64
	timeStepInput int64

	buffer storage.PointBuffer
	reader storage.FileDecoder

	ramvalues *storage.PointBuffer

	columns           []fpsInputColumn
	outputColumnCount int
}

// holds information about the columns stored on disk which should be retrieved
// index on disk is not needed as it's used as map index
type fpsInputColumn struct {
	IndexFile   int
	Transformer encoding.Transformer
	Outputs     []fpsOutputColumn
}

// holds information about a column that was requested by a query
// one is created for each QueryColumn
type fpsOutputColumn struct {
	IndexOuput  int // the index of this QueryColumn
	Downsampler downsampling.Downsampler
}

// read header and find first block that contains points within query range
func (s *FirstPointSource) readHeadersUntil() error {
	header := encoding.BlockHeader{
		TimeLast: math.MinInt64,
	}

	for header.TimeLast < s.timeRange.Start {
		var err error
		header, err = s.reader.DecodeHeader()

		// todo: do proper range check, use TimeRange in BlockHeader
		if err == nil && header.TimeFirst > s.timeRange.End {
			err = io.EOF
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (s *FirstPointSource) readIntoBuffer() error {
	decoded, err := s.reader.DecodeBlock()

	if err != nil {
		return err
	}

	// append values to buffer
	timeNew, err := encoding.TimeTransformer.Revert(decoded[0])
	if err != nil {
		return err
	}

	valuesNew := make([][]int64, len(decoded)-1)
	for i, d := range decoded[1:] {
		valuesNew[i], err = s.columns[i].Transformer.Revert(d)
		if err != nil {
			return err
		}
	}

	s.buffer.AppendBuffer(storage.PointBuffer{
		Time:   timeNew,
		Values: valuesNew,
	})

	return nil
}

//func (s *FirstPointSource) downsampleBuffer() (storage.PointBuffer, error) {
//
//}

func (s *FirstPointSource) Next() (storage.PointBuffer, error) {
	if s.timeRange == nil {
		return storage.PointBuffer{}, io.EOF
	}

	// isEOF is set when the reader returned no new values
	// if this flag is true, the points from the ram buffer are added to the buffer
	var isEOF = false

	// read header and find first block that contains points within query range
	err := s.readHeadersUntil()

	if err == io.EOF {
		isEOF = true
	} else if err != nil {
		return storage.PointBuffer{}, err
	}

	// read block from reader
	if !isEOF {
		err = s.readIntoBuffer()

		if err == io.EOF {
			isEOF = true
		} else if err != nil {
			return storage.PointBuffer{}, err
		}
	}

	if isEOF {
		// append RAM values if bucket reader is at end
		// loop over all points in RAM
		for i, t := range s.ramvalues.Time {
			s.buffer.Time = append(s.buffer.Time, t)
			for ic, c := range s.columns {
				s.buffer.Values[ic] = append(s.buffer.Values[ic], s.ramvalues.Values[c.IndexFile][i])
			}
		}
	}

	// create output array
	output := storage.PointBuffer{
		Time:   make([]int64, 0),
		Values: make([][]int64, s.outputColumnCount),
	}

	for i := range output.Values {
		output.Values[i] = make([]int64, 0)
	}

	// downsample data into output array
	// todo: continue work here
	for s.buffer.Len() > 0 {
		var timeStepStart = util.RoundDown(s.buffer.Time[0], s.timeStep)

		// stop if this time step exceeds timeend
		if timeStepStart > s.timeRange.End {
			isEOF = true
			s.reader.Close()
			break
		}

		// find first point that doesn't belong in this time step anymore
		var indexEnd = -1
		for i, timeend := range s.buffer.Time {
			// enough values if this point is the last of this step
			if timeend == timeStepStart+s.timeStep-s.timeStepInput {
				indexEnd = i + 1
				break
			}
			// enough values if the next value does not belong in this step anymore
			if timeStepStart != util.RoundDown(timeend, s.timeStep) {
				indexEnd = i
				break
			}
		}

		// skip this point if the time step lies before the query range
		if timeStepStart < s.timeRange.Start {
			s.buffer.Discard(indexEnd)
			continue
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
		for iInput, colInput := range s.columns {
			for _, colOutput := range colInput.Outputs {
				val := colOutput.Downsampler.DownsampleFirst(s.buffer.Values[iInput][0:indexEnd])
				output.Values[colOutput.IndexOuput] = append(output.Values[colOutput.IndexOuput], val)
			}
		}

		// update query range
		s.timeRange.Start = timeStepStart + s.timeStep - 1

		// stop if this time step is the last in this query
		if timeStepStart+s.timeStep > s.timeRange.End {
			isEOF = true
			s.reader.Close()
			break
		}

		// remove points from buffer
		s.buffer.Discard(indexEnd)
	}

	if isEOF {
		s.timeRange = nil
	}

	return output, nil
}

// NewFirstPointSource creates a new fps and initializes the bucket reader
// valuesRAM: points that are stored in series.Values
// params.TimeStart gets adjusted to reflect the values already read
func NewFirstPointSource(s *Series, timeRange *TimeRange, queryColumns []QueryColumn, timeStep int64) FirstPointSource {
	// create list of relevant files
	relevantFiles := make([]*storage.DataFile, 0, 8)

	for i, file := range s.FirstBucket.DataFiles {
		if file.TimeEnd < timeRange.Start || file.TimeStart > timeRange.End {
			continue
		}
		relevantFiles = append(relevantFiles, s.FirstBucket.DataFiles[i])
	}

	// determine which columns need to be decoded
	// todo: rename most of this
	inputColumns := make(map[int]*fpsInputColumn)
	decoderColumns := []int{0}

	for i, queryCol := range queryColumns {
		inputCol, exists := inputColumns[queryCol.Index]

		if exists {
			inputCol.Outputs = append(inputCol.Outputs, fpsOutputColumn{
				IndexOuput:  i,
				Downsampler: queryCol.Downsampler,
			})
		} else {
			inputColumns[queryCol.Index] = &fpsInputColumn{
				IndexFile:   queryCol.Index,
				Transformer: s.Columns[queryCol.Index].Transformer,
				Outputs: []fpsOutputColumn{{
					IndexOuput:  i,
					Downsampler: queryCol.Downsampler,
				}},
			}
			decoderColumns = append(decoderColumns, queryCol.Index+1)
		}
	}

	sort.Ints(decoderColumns)

	// fix parameter times to make further roundin operations unnecessary
	// todo: this should probably be somewhere else
	// we only want values where roundDown(time) >= timeStart
	timeRange.Start = util.RoundUp(timeRange.Start, timeStep)
	// we need values after timeEnd, as roundDown(time) might still be <= timeEnd
	// todo: is this + timeStep - 1 necessary
	timeRange.End = util.RoundDown(timeRange.End, timeStep) + timeStep - 1

	// create point source struct
	src := FirstPointSource{
		timeRange:     timeRange,
		timeStep:      timeStep,
		timeStepInput: s.FirstBucket.TimeResolution,

		buffer: storage.NewPointBuffer(len(inputColumns)),
		reader: storage.NewFileDecoder(relevantFiles, decoderColumns),

		ramvalues: &s.Buffer,

		columns:           make([]fpsInputColumn, len(inputColumns)),
		outputColumnCount: 0,
	}

	// copy over columns
	for indexInputCol, indexFile := range decoderColumns[1:] {
		src.columns[indexInputCol] = *inputColumns[indexFile-1]
		src.outputColumnCount += len(inputColumns[indexFile-1].Outputs)
	}

	return src
}
