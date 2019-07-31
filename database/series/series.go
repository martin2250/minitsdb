package series

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/database"
	"io"
	"math"
	"path"
	"strconv"
	"time"

	"github.com/martin2250/minitsdb/database/series/storage"

	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/util"
)

// Column holds the json structure that describes a column in a series
type Column struct {
	Tags        map[string]string
	Decimals    int
	Transformer storage.Transformer
}

// Series describes a time series, id'd by a name and tags
type Series struct {
	Buffer database.PointBuffer

	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Path          string
	Columns       []Column
	Buckets       []Bucket

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

// InsertPoint tries to insert a point into the Series, returns nil if successful
func (s *Series) InsertPoint(p ingest.Point) error {
	// check if number of values matches columns
	if len(p.Values) != len(s.Columns) {
		return ErrColumnMismatch
	}

	// check if points time is already archived
	if p.Time <= s.Buckets[0].TimeLast {
		return ErrInsertAtEnd
	}

	// assign point values to columns
	// indices [index of point.Values] = index of s.Values
	indices := make([]int, len(p.Values))

	// go through all values in point
	for indexValue, value := range p.Values {
		indicesCol := s.GetIndices(value.Tags)

		if len(indicesCol) == 0 {
			return ErrUnknownColumn
		} else if len(indicesCol) != 1 {
			return ErrColumnAmbiguous
		}

		iCol := indicesCol[0] + 1

		for _, iColOther := range indices {
			if iCol == iColOther {
				return ErrColumnMismatch
			}
		}

		indices[indexValue] = iCol // account for time column
	}

	// check if any column didn't receive a value (this shouldn't happen based on previous checks)
	for _, i := range indices {
		if i == 0 {
			return ErrColumnMismatch
		}
	}

	// check if there is already a value at the point's time
	indexBuffer := util.IndexOfInt64(s.Values[0], p.Time)

	if indexBuffer == -1 {
		// time not present in buffer, append to buffer
		indexBuffer = len(s.Values[0])

		for i := range s.Values {
			s.Values[i] = append(s.Values[i], 0)
		}

		s.Values[0][indexBuffer] = p.Time
	}

	// todo: tidy up this hot mess that is indexColumn
	for indexPoint, indexColumn := range indices {
		valf := p.Values[indexPoint].Value
		valf *= math.Pow10(s.Columns[indexColumn-1].Decimals)
		vali := int64(math.Round(valf))
		s.Values[indexColumn][indexBuffer] = vali
	}

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
		FlushDelay: conf.FlushDelay,
		BufferSize: conf.Buffer,
		ReuseMax:   conf.ReuseMax,
		Columns:    make([]Column, 0),
		Buckets:    make([]Bucket, len(conf.Buckets)),
		Tags:       conf.Tags,

		Path: seriespath,
	}

	// create columns
	for _, colconf := range conf.Columns {
		// create transformer
		var transformer storage.Transformer
		if colconf.Transformer != "" {
			var arg int
			if _, err := fmt.Sscanf(colconf.Transformer, "D%d", &arg); err != nil {
				transformer = storage.DiffTransformer{N: arg}
			} else {
				return Series{}, fmt.Errorf("%s matches no known transformers", colconf.Transformer)
			}
		} else {
			// default to differentiating once
			transformer = storage.DiffTransformer{N: 1}
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

		s.Buckets[i] = Bucket{
			TimeLast:       math.MinInt64,
			TimeResolution: timeStep,
			PointsPerFile:  conf.PointsFile,
			First:          i == 0,
			Path:           path.Join(s.Path, strconv.FormatInt(timeStep, 10)),
		}

		err = s.Buckets[i].Init()

		if err != nil {
			return Series{}, err
		}
	}

	// create top level value array
	s.Values = make([][]int64, len(s.Columns))

	// if the last block of the first bucket is not full, read it's values and set the overwrite last flag
	valuesRead, err := s.checkFirstBucket()

	if err != nil {
		return Series{}, err
	}

	// if no values were read, initialize value and time buffers manually
	if !valuesRead {
		for i := range s.Values {
			s.Values[i] = make([]int64, 0)
		}
		s.Time = make([]int64, 0)
	}

	return s, nil
}

// checkFirstBucket checks the last block in the first bucket for free space according to ReuseMax
// errors are to be treated as fatal
func (s *Series) checkFirstBucket() (bool, error) {
	b := &s.Buckets[0]

	// read last block from file
	buf, err := b.getLastBlock()

	if err == io.EOF {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	// decode header
	d := storage.NewDecoder()
	d.SetReader(&buf)

	// read last block header
	header, err := d.DecodeHeader()

	if err != nil {
		return false, err
	}

	// check if database file matches series
	if header.NumColumns != (len(s.Columns) + 1) {
		return false, errors.New("database file does not match series")
	}

	// check if last block can be reused
	if header.BytesUsed > s.ReuseMax {
		return false, nil
	}

	// fill decoder columns
	d.Columns = make([]int, header.NumColumns)
	for i := range d.Columns {
		d.Columns[i] = i
	}

	// read values from last block
	values, err := d.DecodeBlock()

	if err != nil {
		return false, err
	}

	// decode time and values
	s.Time, err = storage.DiffTransformer{N: 2}.Revert(values[0])

	if err != nil {
		return false, err
	}

	for i := range values {
		s.Values[i], err = s.Columns[i].Transformer.Revert(values[i+1])

		if err != nil {
			return false, err
		}
	}

	s.OverwriteLast = true
	return true, nil
}

func (s *Series) CheckFlush() bool {
	if len(s.Values[0]) > s.BufferSize {
		return true
	}

	// todo: also check against last write and flushdelay

	return false
}

// Discard the first n values from RAM
// copy arrays to allow GC to work
func (s *Series) Discard(n int) {
	if n > len(s.Time) {
		n = len(s.Time)
	}
	s.Time = util.Copy1DInt64(s.Time[n:])
	for i := range s.Values {
		s.Values[i] = util.Copy1DInt64(s.Values[i][n:])
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
	overwrite := s.OverwriteLast
	s.OverwriteLast = false
	// don't flush if empty
	if len(s.Time) == 0 {
		return
	}

	b := &s.Buckets[0]

	// check file boundaries
	count, fileTime := b.GetStorageTime(s.Time)

	// transform values
	var err error
	transformed := make([][]uint64, len(s.Values)+1)

	transformed[0], err = storage.TimeTransformer.Apply(s.Time[:count])
	if err != nil {
		// todo: log this properly
		s.SaveDiscard(count)
		fmt.Printf("ERROR while transforming time for series %v\n", s.Tags)
		return
	}

	for i := range s.Values {
		transformed[i+1], err = s.Columns[i].Transformer.Apply(s.Values[i][:count])
		if err != nil {
			// todo: log this properly
			s.SaveDiscard(count)
			fmt.Printf("ERROR while transforming values for series %v\n", s.Tags)
			return
		}
	}

	// encode values
	var buffer bytes.Buffer
	header, err := storage.EncodeBlock(&buffer, s.Time[:count], transformed)

	if err != nil {
		// todo: log this properly
		s.SaveDiscard(count)
		fmt.Printf("ERROR while encoding values for series %v\n", s.Tags)
		return
	}

	// write transformed values to file
	err = s.Buckets[0].WriteBlock(fileTime, buffer.Bytes(), overwrite)

	if err != nil {
		// todo: log this properly
		s.SaveDiscard(header.NumPoints)
		fmt.Printf("ERROR while encoding values for series %v\n", s.Tags)
		return
	}

	// don't discard points if we can reuse the block
	if header.NumPoints == count && header.BytesUsed < s.ReuseMax {
		s.OverwriteLast = true
	} else {
		s.Discard(header.NumPoints)
	}
}
