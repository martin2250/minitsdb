package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"github.com/martin2250/minitsdb/util"
	"io"
	"math"
)

// PrimaryQuerySource reads and aggregates points from the first bucket of a series and RAM
// this is separate from highpointsource (needs renaming even more than this), as both the first bucket and ram don't have pre-downsampled values
type PrimaryQuerySource struct {
	timeRange     *TimeRange
	timeStep      int64
	timeStepInput int64

	// still have time
	buffer           [][]int64
	bufferIndexStart int
	reader           storage.FileDecoder

	ramvalues *storage.PointBuffer

	columns      []QueryColumn
	needIndex    []int
	transformers []encoding.Transformer

	// SkipBlocks has been called yet
	primed bool
}

// read header and find first block that contains points within query range
// do this before calling next for the first time to improve latency
func (s *PrimaryQuerySource) SkipBlocks() error {
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

func (s *PrimaryQuerySource) readIntoBuffer() error {
	header, err := s.reader.DecodeHeader()
	if err != nil {
		return err
	}

	if header.TimeFirst > s.timeRange.End {
		return io.EOF
	}

	decoded, err := s.reader.DecodeBlock()
	if err != nil {
		return err
	}

	// transform values
	transformed := make([][]int64, len(s.transformers))
	for _, i := range s.needIndex {
		transformed[i], err = s.transformers[i].Revert(decoded[i])
		if err != nil {
			return err
		}
	}

	// find indices of first and last relevant point
	indexStart := 0
	indexEnd := len(transformed[0])

	for indexStart < indexEnd && !s.timeRange.Contains(transformed[0][indexStart]) {
		indexStart++
	}
	for indexEnd > indexStart && !s.timeRange.Contains(transformed[0][indexEnd-1]) {
		indexEnd--
	}

	if indexStart == indexEnd {
		return nil
	}

	for _, i := range s.needIndex {
		s.buffer[i] = append(s.buffer[i], transformed[i][indexStart:indexEnd]...)
	}

	return nil
}

func (s *PrimaryQuerySource) Next() (storage.PointBuffer, error) {
	if s.timeRange == nil {
		return storage.PointBuffer{}, io.EOF
	}

	if !s.primed {
		s.SkipBlocks()
		s.primed = true
	}

	err := s.readIntoBuffer()
	if err != nil {
		s.timeRange = nil
		return storage.PointBuffer{}, err
	}

	output := DownsamplePrimary(s.buffer, s.columns, s.timeStep, false, &s.bufferIndexStart)

	if output.Len() > 0 {
		s.timeRange.Start = output.Time[output.Len()-1] + s.timeStep - 1
	}

	// re-use array to reduce allocations
	if s.bufferIndexStart >= len(s.buffer) {
		s.bufferIndexStart = 0
	} else if s.bufferIndexStart > 2*cap(s.buffer[0])/3 {
		for _, i := range s.needIndex {
			length := copy(s.buffer[i], s.buffer[i][s.bufferIndexStart:])
			s.buffer[i] = s.buffer[i][:length]
		}
		s.bufferIndexStart = 0
	}

	return output, nil
}

// NewFirstPointSource creates a new fps and initializes the bucket reader
// valuesRAM: points that are stored in series.Values
// params.TimeStart gets adjusted to reflect the values already read
func NewFirstPointSource(s *Series, timeRange *TimeRange, queryColumns []QueryColumn, timeStep int64) PrimaryQuerySource {
	// create list of relevant files
	relevantFiles := make([]*storage.DataFile, 0, 8)

	for i, file := range s.FirstBucket.DataFiles {
		if file.TimeEnd < timeRange.Start || file.TimeStart > timeRange.End {
			continue
		}
		relevantFiles = append(relevantFiles, s.FirstBucket.DataFiles[i])
	}

	// determine which columns need to be decoded
	decoderNeed := make([]bool, 1+len(s.Columns))
	decoderNeed[0] = true // need time
	transformers := make([]encoding.Transformer, 1+len(s.Columns))
	transformers[0] = encoding.TimeTransformer

	for _, queryCol := range queryColumns {
		decoderNeed[queryCol.Column.IndexPrimary] = true
		transformers[queryCol.Column.IndexPrimary] = queryCol.Column.Transformer
	}

	needIndex := make([]int, 0)
	for i, need := range decoderNeed {
		if need {
			needIndex = append(needIndex, i)
		}
	}

	// fix parameter times to make further roundin operations unnecessary
	// todo: this should probably be somewhere else
	// we only want values where roundDown(time) >= timeStart
	timeRange.Start = util.RoundUp(timeRange.Start, timeStep)
	// we need values after timeEnd, as roundDown(time) might still be <= timeEnd
	// todo: is this + timeStep - 1 necessary
	timeRange.End = util.RoundDown(timeRange.End, timeStep) + timeStep - 1

	// create point source struct
	src := PrimaryQuerySource{
		timeRange:     timeRange,
		timeStep:      timeStep,
		timeStepInput: s.FirstBucket.TimeResolution,

		buffer: make([][]int64, len(decoderNeed)),
		reader: storage.NewFileDecoder(relevantFiles, decoderNeed),

		ramvalues: &s.Buffer,

		columns:      queryColumns,
		needIndex:    needIndex,
		transformers: transformers,
	}

	return src
}
