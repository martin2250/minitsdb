package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"github.com/martin2250/minitsdb/util"
	"io"
	"math"
	"sort"
)

type IndexedQueryColumn struct {
	Index       int // the index of this column in the file decoder
	QueryColumn QueryColumn
}

// PrimaryQuerySource reads and aggregates points from the first bucket of a series and RAM
// this is separate from highpointsource (needs renaming even more than this), as both the first bucket and ram don't have pre-downsampled values
type PrimaryQuerySource struct {
	timeRange     *TimeRange
	timeStep      int64
	timeStepInput int64

	buffer storage.PointBuffer
	reader storage.FileDecoder

	ramvalues   *storage.PointBuffer
	ramvaluemap []int

	columns []IndexedQueryColumn
}

// read header and find first block that contains points within query range
func (s *PrimaryQuerySource) readHeadersUntil() error {
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
		valuesNew[i], err = s.columns[i].QueryColumn.Column.Transformer.Revert(d)
		if err != nil {
			return err
		}
	}

	b := storage.PointBuffer{
		Time:   timeNew,
		Values: valuesNew,
	}

	b.TrimStart(s.timeRange.Start)
	b.TrimEnd(s.timeRange.End)

	s.buffer.AppendBuffer(b)

	return nil
}

//func (s *FirstPointSource) downsampleBuffer() (storage.PointBuffer, error) {
//
//}

func (s *PrimaryQuerySource) Next() (storage.PointBuffer, error) {
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
			if s.timeRange.Contains(t) {
				index := s.buffer.InsertIndex(t)
				for ib, ir := range s.ramvaluemap {
					s.buffer.Values[ib][index] = s.ramvalues.Values[ir][i]
				}
			}
		}
	}

	// create output array
	output := storage.PointBuffer{
		Time:   make([]int64, 0),
		Values: make([][]int64, len(s.columns)),
	}

	for i := range output.Values {
		output.Values[i] = make([]int64, 0)
	}

	for s.buffer.Len() > 0 {
		// downsample data into output array
		var currentTimeStep TimeRange
		{
			// don't know if this makes a difference
			x := util.RoundDown(s.buffer.Time[0], s.timeStep)
			currentTimeStep.Start = x
			currentTimeStep.End = x + s.timeStep - 1
		}

		// stop if this time step exceeds timeend
		if currentTimeStep.Start > s.timeRange.End {
			isEOF = true
			s.reader.Close()
			break
		}

		// find first point that doesn't belong in this time step anymore
		var indexEnd = -1
		for i, timeend := range s.buffer.Time {
			// enough values if this point is the last of this step
			if timeend == currentTimeStep.End {
				indexEnd = i + 1
				break
			}
			// enough values if the next value does not belong in this step anymore
			if !currentTimeStep.Contains(timeend) {
				indexEnd = i
				break
			}
		}

		// skip this point if the time step lies before the query range
		if currentTimeStep.Start < s.timeRange.Start {
			s.buffer.Discard(indexEnd)
			continue
		}

		// not enough values to fill this time step
		if indexEnd == -1 {
			if isEOF {
				// this is firstpointsource, so there are no more points to complete this time step, just use all available values
				indexEnd = len(s.buffer.Time)
			} else {
				// keep values for next call to Next()
				break
			}
		}

		// append time of point to output
		output.Time = append(output.Time, currentTimeStep.Start)
		// append downsampled values
		for i, indexedQueryColumn := range s.columns {
			val := indexedQueryColumn.QueryColumn.Function.AggregatePrimary(s.buffer.Values[indexedQueryColumn.Index][0:indexEnd], s.buffer.Time[0:indexEnd])
			output.Values[i] = append(output.Values[i], val)
		}

		// update query range
		s.timeRange.Start = currentTimeStep.End + 1

		// stop if this time step is the last in this query
		if s.timeRange.Start > s.timeRange.End {
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
	decoderColumnsSet := make(map[int]struct{}, len(s.Columns))
	for _, queryCol := range queryColumns {
		decoderColumnsSet[queryCol.Column.IndexPrimary] = struct{}{}
	}

	ramvaluemap := make([]int, 0, len(decoderColumnsSet))
	decoderColumns := make([]int, 1, len(decoderColumnsSet)+1)
	decoderColumns[0] = 0 // always decode time
	for i := range decoderColumnsSet {
		decoderColumns = append(decoderColumns, i)
		ramvaluemap = append(ramvaluemap, i-1)
	}
	sort.Ints(decoderColumns)
	sort.Ints(ramvaluemap)

	//
	indexedColumns := make([]IndexedQueryColumn, len(queryColumns))
	for i, queryCol := range queryColumns {
		indexedColumns[i] = IndexedQueryColumn{
			Index:       util.IndexOfInt(decoderColumns, queryCol.Column.IndexPrimary) - 1,
			QueryColumn: queryCol,
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

		buffer: storage.NewPointBuffer(len(decoderColumnsSet)),
		reader: storage.NewFileDecoder(relevantFiles, decoderColumns),

		ramvalues:   &s.Buffer,
		ramvaluemap: ramvaluemap,

		columns: indexedColumns,
	}

	return src
}
