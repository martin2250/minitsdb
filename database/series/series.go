package series

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"regexp"
	"time"

	"github.com/golang/glog"

	"github.com/martin2250/minitsdb/encoder"

	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/util"
)

// Column holds the json structure that describes a column in a series
type Column struct {
	Tags     map[string]string
	Decimals int
}

// Series describes a time series, id'd by a name and tags
type Series struct {
	Values        [][]int64
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
	indices := make([]int, len(s.Columns))
	// set all to -1 to indicate that no matching value was found in point (yet)
	for i := range indices {
		indices[i] = -1
	}
	// go through all values in point
	for indexValue, value := range p.Values {
		indicesCol := s.GetIndices(value.Tags)

		if len(indicesCol) == 0 {
			return ErrUnknownColumn
		} else if len(indicesCol) != 1 {
			return ErrColumnAmbiguous
		}

		indexColumn := indicesCol[0]

		// check two points match this column
		if indices[indexColumn] != -1 {
			return ErrColumnMismatch
		}

		indices[indexColumn] = indexValue
	}

	// check if any column didn't receive a value (this shouldn't happen based on previous checks)
	for _, i := range indices {
		if i == -1 {
			return ErrColumnMismatch
		}
	}

	for indexColumn, indexPoint := range indices {
		valf := p.Values[indexPoint].Value
		valf *= math.Pow10(s.Columns[indexColumn].Decimals)
		vali := int64(math.Round(valf))
		s.Values[indexColumn] = append(s.Values[indexColumn], vali)
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

			ok, _ = regexp.MatchString(queryValue, columnValue)
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

	// create buckets
	timeStep := int64(1)

	for i, bc := range conf.Buckets {
		timeStep *= int64(bc.Factor)

		s.Buckets[i].series = &s
		s.Buckets[i].TimeResolution = timeStep

		s.Buckets[i].First = (i == 0)

		if err := s.Buckets[i].checkTimeLast(); err != nil {
			return Series{}, err
		}
	}

	// create columns
	for _, colconf := range conf.Columns {
		if colconf.Duplicate == nil {
			s.Columns = append(s.Columns, Column{
				Decimals: colconf.Decimals,
				Tags:     colconf.Tags,
			})
		} else {
			for _, tagset := range colconf.Duplicate {
				col := Column{
					Decimals: colconf.Decimals,
					Tags:     make(map[string]string),
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
				return Series{}, fmt.Errorf("Columns %v and %v are indistinguishable", a.Tags, b.Tags)
			}
		}
	}

	err = s.checkFirstBucket()

	if err != nil {
		return Series{}, err
	}

	if s.Values == nil {
		// create value buffers, 1 extra column for time
		s.Values = make([][]int64, 1+len(s.Columns))

		for i := range s.Values {
			s.Values[i] = make([]int64, 0)
		}
	}

	return s, nil
}

// checkFirstBucket checks the last block in the first bucket for free space according to ReuseMax
// errors are to be treated as fatal
func (s *Series) checkFirstBucket() error {
	b := &s.Buckets[0]

	times, err := b.GetDataFiles()

	if err != nil {
		return err
	}

	// no file to read
	if len(times) < 1 {
		glog.Info("no files in bucket")
		return nil
	}

	lastPath := b.GetFileName(times[len(times)-1])

	// check file size
	stat, err := os.Stat(lastPath)

	if err != nil {
		return err
	}

	size := stat.Size()

	if size%4096 != 0 {
		return errors.New("file damaged")
	}

	blocks := size / 4096

	// no blocks to read
	if blocks < 1 {
		return nil
	}

	// open file
	file, err := os.Open(lastPath)

	if err != nil {
		return err
	}

	defer file.Close()

	// seek last block
	if _, err = file.Seek((blocks-1)*4096, io.SeekStart); err != nil {
		return err
	}

	// read last block
	header, values, err := encoder.ReadBlock(file)

	if err != nil {
		return err
	}

	// check if database file matches series
	if int(header.NumColumns) != len(s.Columns) {
		return errors.New("database file does not match series")
	}

	// reuse last block
	if int(header.BytesUsed) < s.ReuseMax {
		s.OverwriteLast = true
		s.Values = values
	}

	return nil
}
