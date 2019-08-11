package series

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/database/series/query"
	"github.com/martin2250/minitsdb/database/series/storage"
	"github.com/martin2250/minitsdb/database/series/storage/encoding"
	"math"
	"time"

	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/util"
)

// Column holds the json structure that describes a column in a series
type Column struct {
	Tags        map[string]string
	Decimals    int
	Transformer encoding.Transformer
}

// Series describes a time series, id'd by a name and tags
type Series struct {
	// todo: make buffer private
	Buffer storage.PointBuffer

	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Path          string
	Columns       []Column
	FirstBucket   storage.Bucket
	LastBuckets   []storage.Bucket

	// LastFlush   time.Time
	OldestValue int64

	Tags       map[string]string
	FlushDelay time.Duration
	BufferSize int
	ReuseMax   int
}

// ErrColumnMismatch indicates that the insert failed because point values could not be assigned to series columns unambiguously
var ErrColumnMismatch = errors.New("point values could not be assigned to series columns unambiguously")

// ErrInsertAtEnd indicates that the insert failed because the point's time is already archived in a file
var ErrInsertAtEnd = errors.New("time already archived")

// ErrColumnAmbiguous indicates that the insert failed because point value tags match two columns
var ErrColumnAmbiguous = errors.New("point values matches two columns")

// ErrUnknownColumn indicates that the insert failed because one of the values could not be assigned to a column
var ErrUnknownColumn = errors.New("value doesn't match any columns")

// ConvertPoint converts a point from the ingest format (values and maps of tags) to the series format (list of values in fixed order)
func (s Series) ConvertPoint(p ingest.Point) (storage.Point, error) {
	// number of values must match
	if len(p.Values) != len(s.Columns) {
		return storage.Point{}, ErrColumnMismatch
	}

	// holds output value
	out := storage.Point{
		Time:   p.Time,
		Values: make([]int64, len(s.Columns)),
	}

	// true for every column that has already been assigned a value,
	// used to check if two values from p match the same column
	filled := make([]bool, len(s.Columns))

	for _, v := range p.Values {
		indices := s.GetIndices(v.Tags)

		if len(indices) == 0 {
			return storage.Point{}, ErrUnknownColumn
		} else if len(indices) != 1 {
			return storage.Point{}, ErrColumnAmbiguous
		}

		i := indices[0]

		if filled[i] {
			return storage.Point{}, ErrColumnMismatch
		}

		filled[i] = true

		valf := v.Value * math.Pow10(s.Columns[i].Decimals)
		out.Values[i] = int64(math.Round(valf))
	}

	return out, nil
}

// InsertPoint tries to insert a point into the Series, returns nil if successful
func (s *Series) InsertPoint(p storage.Point) error {
	// check if number of values matches columns
	if len(p.Values) != len(s.Columns) {
		return ErrColumnMismatch
	}

	// check if points time is already archived
	if p.Time <= s.FirstBucket.TimeLast {
		return ErrInsertAtEnd
	}

	if p.Time < s.OldestValue {
		s.OldestValue = p.Time
	}

	s.Buffer.InsertPoint(p)

	return nil
}

// GetIndices returns the indices of all columns that match the given set of tags
// the values of argument 'tags' are used as regex to match against all columns
func (s Series) GetIndices(tags map[string]string) []int {
	indices := make([]int, 0)

	for i, column := range s.Columns {
		matches := true

		for queryKey, queryValue := range tags {
			columnValue, ok := column.Tags[queryKey]
			if !ok {
				matches = false
				break
			}

			//ok, _ = regexp.MatchString(queryValue, columnValue)
			ok = queryValue == columnValue
			if !ok {
				matches = false
				break
			}
		}

		if matches {
			indices = append(indices, i)
		}
	}

	return indices
}

// OpenSeries opens series from file
func OpenSeries(seriespath string) (Series, error) {
	// load config file
	conf, err := LoadSeriesYamlConfig(seriespath)

	if err != nil {
		return Series{}, err
	}

	// create series struct
	s := Series{
		FlushDelay:  conf.FlushDelay,
		BufferSize:  conf.Buffer,
		ReuseMax:    conf.ReuseMax,
		Columns:     make([]Column, 0),
		LastBuckets: make([]storage.Bucket, len(conf.Buckets)),
		Tags:        conf.Tags,
		OldestValue: math.MaxInt64,

		Path: seriespath,
	}

	// create columns
	for _, colconf := range conf.Columns {
		// create transformer
		var transformer encoding.Transformer
		if colconf.Transformer != "" {
			var arg int
			if _, err := fmt.Sscanf(colconf.Transformer, "D%d", &arg); err != nil {
				transformer = encoding.DiffTransformer{N: arg}
			} else {
				return Series{}, fmt.Errorf("%s matches no known transformers", colconf.Transformer)
			}
		} else {
			// default to differentiating once
			transformer = encoding.DiffTransformer{N: 1}
		}

		if colconf.Duplicate == nil {
			s.Columns = append(s.Columns, Column{
				Decimals:    colconf.Decimals,
				Tags:        colconf.Tags,
				Transformer: transformer,
			})
		} else {
			for _, tagset := range colconf.Duplicate {
				col := Column{
					Decimals:    colconf.Decimals,
					Tags:        make(map[string]string),
					Transformer: transformer,
				}

				for tag, value := range colconf.Tags {
					col.Tags[tag] = value
				}

				for tag, value := range tagset {
					col.Tags[tag] = value
				}
				s.Columns = append(s.Columns, col)
			}
		}
	}

	// check columns for duplicates
	for ia, a := range s.Columns {
		for ib, b := range s.Columns {
			if ia == ib {
				continue
			}
			if util.IsSubset(a.Tags, b.Tags) {
				return Series{}, fmt.Errorf("columns %v and %v are indistinguishable", a.Tags, b.Tags)
			}
		}
	}

	// create buckets
	timeStep := int64(1)

	for i, bc := range conf.Buckets {
		timeStep *= int64(bc.Factor)

		s.LastBuckets[i], err = storage.OpenBucket(s.Path, timeStep, conf.PointsFile)

		if err != nil {
			return Series{}, err
		}
	}

	// split away first bucket
	s.FirstBucket = s.LastBuckets[0]
	s.LastBuckets = s.LastBuckets[1:]

	// create top level value array
	s.Buffer = storage.NewPointBuffer(len(s.Columns))

	// if the last block of the first bucket is not full, read it's values and set the overwrite last flag
	err = s.checkFirstBucket()

	if err != nil {
		return Series{}, err
	}

	return s, nil
}

// checkFirstBucket checks the last block in the first bucket for free space according to ReuseMax
// errors are to be treated as fatal
func (s *Series) checkFirstBucket() error {
	if len(s.FirstBucket.DataFiles) == 0 {
		return nil
	}
	// read last block from file
	dataFile := s.FirstBucket.DataFiles[len(s.FirstBucket.DataFiles)-1]
	buf, err := dataFile.ReadBlock(dataFile.Blocks - 1)

	if err != nil {
		return err
	}

	// decode header
	d := encoding.NewDecoder()
	d.SetReader(&buf)

	// read last block header
	header, err := d.DecodeHeader()

	if err != nil {
		return err
	}

	// check if database file matches series
	if header.NumColumns != (len(s.Columns) + 1) {
		return errors.New("database file does not match series")
	}

	// check if last block can be reused
	if header.BytesUsed > s.ReuseMax {
		return nil
	}

	// fill decoder columns
	d.Columns = make([]int, header.NumColumns)
	for i := range d.Columns {
		d.Columns[i] = i
	}

	// read values from last block
	values, err := d.DecodeBlock()

	if err != nil {
		return err
	}

	// decode time and values
	s.Buffer.Time, err = encoding.TimeTransformer.Revert(values[0])

	if err != nil {
		return err
	}

	for i := range s.Columns {
		s.Buffer.Values[i], err = s.Columns[i].Transformer.Revert(values[i+1])

		if err != nil {
			return err
		}
	}

	s.OverwriteLast = true
	return nil
}

func (s *Series) CheckFlush() bool {
	if s.Buffer.Len() > s.BufferSize {
		return true
	}

	// todo: also check against last write and flushdelay

	return false
}

// Discard the first n values from RAM
// copy arrays to allow GC to work
func (s *Series) Discard(n int) {
	s.Buffer.Discard(n)

	if s.Buffer.Len() == 0 {
		s.OldestValue = math.MaxInt64
	} else {
		s.OldestValue = s.Buffer.Time[0]
	}
}

// SaveDiscard saves n values to a recovery file and then calls discard
func (s *Series) SaveDiscard(n int) {
	// todo: store raw values in recovery file
	s.Discard(n)
}

// todo: make this only flush after
// todo: a) a configurable amount of time after the last flush
// todo: b) or a configurable maximum amount of values is in the buffer
// Flush does not return an error, errors are handled by the function itself
func (s *Series) Flush() {
	if s.Buffer.Len() == 0 {
		return
	}

	fmt.Printf("flushing series %v with %d points\n", s.Tags, s.Buffer.Len())

	// check file boundaries
	dataFile, count := s.FirstBucket.GetStorageTime(s.Buffer.Time)

	// transform values
	var err error
	transformed := make([][]uint64, len(s.Columns)+1)

	transformed[0], err = encoding.TimeTransformer.Apply(s.Buffer.Time[:count])
	if err != nil {
		fmt.Printf("ERROR while transforming time for series %v %s\n", s.Tags, err.Error())
		s.SaveDiscard(count) // todo: log this properly
		return
	}

	for i := range s.Columns {
		transformed[i+1], err = s.Columns[i].Transformer.Apply(s.Buffer.Values[i][:count])
		if err != nil {
			fmt.Printf("ERROR while transforming values for series %v %s\n", s.Tags, err.Error())
			s.SaveDiscard(count) // todo: log this properly
			return
		}
	}

	// encode values
	var block bytes.Buffer
	header, err := encoding.EncodeBlock(&block, s.Buffer.Time[:count], transformed)

	if err != nil {
		s.SaveDiscard(count) // todo: log this properly
		fmt.Printf("ERROR while encoding values for series %v %s\n", s.Tags, err.Error())
		return
	}

	// write transformed values to file
	err = dataFile.WriteBlock(block, s.OverwriteLast)

	if err != nil {
		s.SaveDiscard(header.NumPoints) // todo: log this properly
		fmt.Printf("ERROR while encoding values for series %v %s\n", s.Tags, err.Error())
		return
	}

	fmt.Printf("wrote %d points to file, block size %d bytes", header.NumPoints, header.BytesUsed)

	// don't discard points if we can reuse the block
	// todo: fix overwriting, right now it seems to always append to the file
	if false && header.NumPoints == s.Buffer.Len() && header.BytesUsed < s.ReuseMax {
		s.OverwriteLast = true
		fmt.Println(", reusing buffer")
	} else {
		s.FirstBucket.TimeLast = s.Buffer.Time[count-1]
		s.OverwriteLast = false
		s.Discard(header.NumPoints)
		fmt.Println(", flushing buffer")
	}
}

func (s *Series) Query(params query.Parameters) *query.Query {
	// adjust param time step to be an integer multiple of a bucket time step
	if params.TimeStep < 1 {
		params.TimeStep = 1
	}

	params.TimeStep = util.RoundUp(params.TimeStep, s.FirstBucket.TimeResolution)

	for _, bucket := range s.LastBuckets {
		if bucket.TimeResolution <= params.TimeStep {
			params.TimeStep = util.RoundUp(params.TimeStep, bucket.TimeResolution)
		}
	}

	// create query object
	q := &query.Query{
		Param:   params,
		Sources: make([]query.PointSource, 0, 1),
	}

	// create first point source
	// todo: add sources for other buckets
	fpsRamValues := storage.NewPointBuffer(len(params.Columns))
	fpsTransformers := make([]encoding.Transformer, len(params.Columns))

	fpsRamValues.Time = s.Buffer.Time
	for i, col := range params.Columns {
		fpsRamValues.Values[i] = s.Buffer.Values[col.Index]
		fpsTransformers[i] = s.Columns[col.Index].Transformer
	}

	fps := query.NewFirstPointSource(s.FirstBucket.DataFiles, &q.Param, fpsRamValues, fpsTransformers)
	q.Sources = append(q.Sources, &fps)

	return q
}
