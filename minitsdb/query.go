package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"github.com/martin2250/minitsdb/util"
	"io"
	"math"
)

// QueryColumn is the combination of a column index and aggregation
type QueryColumn struct {
	Column   *Column
	Function downsampling.Function
}

// Query reads and aggregates points from a bucket of a series (both from disk and RAM)
type Query struct {
	timeRange TimeRange
	timeStep  int64

	buffer           storage.PointBuffer
	bufferIndexStart int
	reader           storage.FileDecoder

	bucket *Bucket

	columns      []QueryColumn
	needIndex    []int
	transformers []encoding.Transformer

	// SkipBlocks has been called yet
	primed bool
	atEnd  bool
}

// read header and find first block that contains points within query range
// do this before calling next for the first time to improve latency
func (q *Query) SkipBlocks() error {
	header := encoding.BlockHeader{
		TimeLast: math.MinInt64,
	}

	for header.TimeLast < q.timeRange.Start {
		var err error
		header, err = q.reader.DecodeHeader()

		if err != nil {
			return err
		}

		// todo: do proper range check, use TimeRange in BlockHeader
		if header.TimeFirst > q.timeRange.End {
			err = io.EOF
		}
	}

	return nil
}

func (q *Query) readIntoBuffer() error {
	decoded, err := q.reader.DecodeBlock()
	if err != nil {
		return err
	}

	// transform values
	transformed := make([][]int64, len(q.transformers))
	for _, i := range q.needIndex {
		transformed[i], err = q.transformers[i].Revert(decoded[i])
		if err != nil {
			return err
		}
	}

	// find indices of first and last relevant point
	indexStart := 0
	indexEnd := len(transformed[0])

	for indexStart < indexEnd && !q.timeRange.Contains(transformed[0][indexStart]) {
		indexStart++
	}
	for indexEnd > indexStart && !q.timeRange.Contains(transformed[0][indexEnd-1]) {
		indexEnd--
	}

	if indexStart == indexEnd {
		return nil
	}

	for _, i := range q.needIndex {
		q.buffer.Values[i] = append(q.buffer.Values[i], transformed[i][indexStart:indexEnd]...)
	}

	return nil
}

func (q *Query) Next() (storage.PointBuffer, error) {
	if q.atEnd {
		return storage.PointBuffer{}, io.EOF
	}

	if q.timeRange.Start >= q.timeRange.End {
		return storage.PointBuffer{}, io.EOF
	}

	if !q.primed {
		err := q.SkipBlocks()
		if err == io.EOF {
			q.atEnd = true
		} else if err != nil {
			return storage.PointBuffer{}, err
		}
		q.primed = true
	}

	if !q.atEnd {
		err := q.readIntoBuffer()
		if err != nil {
			if err != io.EOF {
				return storage.PointBuffer{}, err
			}
			q.atEnd = true
		}
	}

	if q.atEnd {
		for i := range q.bucket.Buffer.Values[0] {
			if q.timeRange.Contains(q.bucket.Buffer.Values[0][i]) {
				q.buffer.InsertPoint(q.bucket.Buffer.At(i))
			}
		}
	}

	output := DownsampleQuery(q.buffer, q.columns, q.timeStep, false, &q.bufferIndexStart, q.bucket.First)

	if output.Len() > 0 {
		q.timeRange.Start = output.Values[0][output.Len()-1] + q.timeStep
	}

	// re-use array to reduce allocations
	if q.bufferIndexStart >= len(q.buffer.Values[0]) {
		q.bufferIndexStart = 0
		for _, i := range q.needIndex {
			q.buffer.Values[i] = q.buffer.Values[i][:0]
		}
	} else if q.bufferIndexStart > 2*cap(q.buffer.Values[0])/3 {
		for _, i := range q.needIndex {
			length := copy(q.buffer.Values[i], q.buffer.Values[i][q.bufferIndexStart:])
			q.buffer.Values[i] = q.buffer.Values[i][:length]
		}
		q.bufferIndexStart = 0
	}

	return output, nil
}

func (b *Bucket) Query(columns []QueryColumn, timeRange TimeRange, timeStep int64) *Query {
	// create list of relevant files
	relevantFiles := make([]*storage.DataFile, 0, 8)

	for i, file := range b.DataFiles {
		if file.TimeEnd < timeRange.Start || file.TimeStart > timeRange.End {
			continue
		}
		relevantFiles = append(relevantFiles, b.DataFiles[i])
	}

	// determine which columns need to be decoded
	var decoderNeed = make([]bool, b.Buffer.Cols())
	var transformers = make([]encoding.Transformer, b.Buffer.Cols())

	decoderNeed[0] = true // need time
	transformers[0] = encoding.TimeTransformer

	if b.First {
		for _, queryCol := range columns {
			decoderNeed[queryCol.Column.IndexPrimary] = true
			transformers[queryCol.Column.IndexPrimary] = queryCol.Column.Transformer
		}
	} else {
		decoderNeed[1] = true
		transformers[1] = encoding.CountTransformer

		for _, queryCol := range columns {
			need := make([]bool, downsampling.AggregatorCount)
			queryCol.Function.Needs(need)
			for i, indexSecondary := range queryCol.Column.IndexSecondary {
				if need[i] {
					decoderNeed[indexSecondary] = true
					transformers[indexSecondary] = queryCol.Column.Transformer
				}
			}
		}
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
	query := Query{
		timeRange: timeRange,
		timeStep:  util.RoundUp(timeStep, b.TimeStep),

		buffer: storage.NewPointBuffer(b.Buffer.Cols()),
		reader: storage.NewFileDecoder(relevantFiles, decoderNeed),

		columns:      columns,
		needIndex:    needIndex,
		transformers: transformers,

		bucket: b,
	}

	query.buffer.Need = decoderNeed

	return &query
}
