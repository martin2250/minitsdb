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
type BucketQuerySource struct {
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

	primary bool
}

// read header and find first block that contains points within query range
// do this before calling next for the first time to improve latency
func (bs *BucketQuerySource) SkipBlocks() error {
	header := encoding.BlockHeader{
		TimeLast: math.MinInt64,
	}

	for header.TimeLast < bs.timeRange.Start {
		var err error
		header, err = bs.reader.DecodeHeader()

		// todo: do proper range check, use TimeRange in BlockHeader
		if err == nil && header.TimeFirst > bs.timeRange.End {
			err = io.EOF
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *BucketQuerySource) readIntoBuffer() error {
	//header, err := bs.reader.DecodeHeader()
	//if err != nil {
	//	return err
	//}

	decoded, err := bs.reader.DecodeBlock()
	if err != nil {
		return err
	}

	// transform values
	transformed := make([][]int64, len(bs.transformers))
	for _, i := range bs.needIndex {
		transformed[i], err = bs.transformers[i].Revert(decoded[i])
		if err != nil {
			return err
		}
	}

	// find indices of first and last relevant point
	indexStart := 0
	indexEnd := len(transformed[0])

	for indexStart < indexEnd && !bs.timeRange.Contains(transformed[0][indexStart]) {
		indexStart++
	}
	for indexEnd > indexStart && !bs.timeRange.Contains(transformed[0][indexEnd-1]) {
		indexEnd--
	}

	if indexStart == indexEnd {
		return nil
	}

	for _, i := range bs.needIndex {
		bs.buffer[i] = append(bs.buffer[i], transformed[i][indexStart:indexEnd]...)
	}

	return nil
}

func (bs *BucketQuerySource) Next() (storage.PointBuffer, error) {
	if bs.timeRange == nil {
		return storage.PointBuffer{}, io.EOF
	}

	if !bs.primed {
		err := bs.SkipBlocks()
		if err != nil {
			bs.timeRange = nil
			return storage.PointBuffer{}, err
		}
		bs.primed = true
	}

	err := bs.readIntoBuffer()
	if err != nil {
		bs.timeRange = nil
		return storage.PointBuffer{}, err
	}

	output := DownsampleQuery(bs.buffer, bs.columns, bs.timeStep, false, &bs.bufferIndexStart, bs.primary)

	if output.Len() > 0 {
		bs.timeRange.Start = output.Time[output.Len()-1] + bs.timeStep - 1
	}

	// re-use array to reduce allocations
	if bs.bufferIndexStart >= len(bs.buffer[0]) {
		bs.bufferIndexStart = 0
	} else if bs.bufferIndexStart > 2*cap(bs.buffer[0])/3 {
		for _, i := range bs.needIndex {
			length := copy(bs.buffer[i], bs.buffer[i][bs.bufferIndexStart:])
			bs.buffer[i] = bs.buffer[i][:length]
		}
		bs.bufferIndexStart = 0
	}

	return output, nil
}

// NewBucketQuerySource creates a new fps and initializes the bucket reader
// valuesRAM: points that are stored in series.Values
// params.TimeStart gets adjusted to reflect the values already read
func NewBucketQuerySource(s *Series, bucket *storage.Bucket, timeRange *TimeRange, queryColumns []QueryColumn, timeStep int64, primary bool) BucketQuerySource {
	// create list of relevant files
	relevantFiles := make([]*storage.DataFile, 0, 8)

	for i, file := range bucket.DataFiles {
		if file.TimeEnd < timeRange.Start || file.TimeStart > timeRange.End {
			continue
		}
		relevantFiles = append(relevantFiles, bucket.DataFiles[i])
	}

	// determine which columns need to be decoded
	var decoderNeed []bool
	var transformers []encoding.Transformer

	if primary {
		decoderNeed = make([]bool, s.PrimaryCount)
		transformers = make([]encoding.Transformer, s.PrimaryCount)

		for _, queryCol := range queryColumns {
			decoderNeed[queryCol.Column.IndexPrimary] = true
			transformers[queryCol.Column.IndexPrimary] = queryCol.Column.Transformer
		}
	} else {
		decoderNeed = make([]bool, s.SecondaryCount)
		transformers = make([]encoding.Transformer, s.SecondaryCount)

		decoderNeed[1] = true
		transformers[1] = encoding.CountTransformer

		for _, queryCol := range queryColumns {
			for _, indexSecondary := range queryCol.Column.IndexSecondary {
				decoderNeed[indexSecondary] = true
				transformers[indexSecondary] = queryCol.Column.Transformer
			}
		}
	}

	decoderNeed[0] = true // need time
	transformers[0] = encoding.TimeTransformer

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
	src := BucketQuerySource{
		timeRange:     timeRange,
		timeStep:      timeStep,
		timeStepInput: bucket.TimeResolution,

		buffer: make([][]int64, len(decoderNeed)),
		reader: storage.NewFileDecoder(relevantFiles, decoderNeed),

		ramvalues: &s.Buffer,

		columns:      queryColumns,
		needIndex:    needIndex,
		transformers: transformers,

		primary: primary,
	}

	return src
}
