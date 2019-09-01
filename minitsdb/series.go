package minitsdb

import (
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
	"github.com/martin2250/minitsdb/util"
)

// Column holds the json structure that describes a column in a series
type Column struct {
	Tags        map[string]string
	Decimals    int
	Transformer encoding.Transformer

	// IndexPrimary holds the index of this column in the primary bucket
	// IndexPrimary starts at one to account for the time column
	IndexPrimary int
	// IndexSecondary holds the indices of all downsampled versions of this column
	// in the secondary buckets. It contains one such index for every downsampling
	// type registered to the system. Unused downsampling types have index 0
	// IndexSecondary starts at two to account for time and count columns
	IndexSecondary []int

	DefaultFunction downsampling.Function
}

func (c Column) Supports(f downsampling.Function) bool {
	need := make([]bool, downsampling.AggregatorCount)
	f.Needs(need)
	for i, n := range need {
		if n && c.IndexSecondary[i] == 0 {
			return false
		}
	}
	return true
}

// Series describes a time series, id'd by a name and tags
type Series struct {
	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Path          string
	Columns       []Column

	Buckets []Bucket

	Tags map[string]string

	OldestValue int64

	LastFlush     time.Time
	FlushInterval time.Duration

	FlushCount      int
	ForceFlushCount int

	ReuseMax int

	PrimaryCount   int
	SecondaryCount int
}

type AssociatedPoint struct {
	Point  storage.Point
	Series *Series
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
		Values: make([]int64, len(s.Columns)+1),
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
		out.Values[i+1] = int64(math.Round(valf))
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
	if p.Values[0] <= s.Buckets[0].LastTimeOnDisk {
		return ErrInsertAtEnd
	}

	if p.Values[0] < s.OldestValue {
		s.OldestValue = p.Values[0]
	}

	s.Buckets[0].Insert(p)

	return nil
}

// GetIndices returns the indices of all columns that match the given set of tags
// the values of argument 'tags' are used as regex to match against all columns
// todo: deprecate in favor of findcolumns
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

// FindColumns finds all columns that match the given set of tags
// if useRegex is true, all tag values of format /.../ as treated as regexes
func (s Series) FindColumns(tags map[string]string, useRegex bool) []*Column {
	columns := make([]*Column, 0)
Top:
	for i, column := range s.Columns {
		for queryKey, queryValue := range tags {
			columnValue, ok := column.Tags[queryKey]
			if !ok {
				continue Top
			}

			if useRegex && strings.HasPrefix(queryValue, "/") && strings.HasSuffix(queryValue, "/") {
				ok, _ = regexp.MatchString(queryValue[1:len(queryValue)-1], columnValue)
			} else {
				ok = queryValue == columnValue
			}

			if !ok {
				continue Top
			}
		}
		columns = append(columns, &s.Columns[i])
	}

	return columns
}

func (s *Series) addColumn(conf YamlColumnConfig) error {
	col := Column{
		Decimals: conf.Decimals,
	}

	// find transformer
	if conf.Transformer == "" {
		col.Transformer = encoding.DiffTransformer{N: 1}
	} else {
		var err error
		col.Transformer, err = encoding.FindTransformer(conf.Transformer)
		if err != nil {
			return err
		}
	}

	// find aggregations
	needs := make([]bool, downsampling.AggregatorCount)

	if len(conf.Aggregations) == 0 {
		// default to storing the mean
		needs[downsampling.Mean.GetIndex()] = true
	} else {
		for _, as := range conf.Aggregations {
			a, ok := downsampling.Aggregators[as]
			if !ok {
				return fmt.Errorf("aggregator %s not found", as)
			}
			a.Needs(needs)
		}
	}

	if conf.Duplicate == nil {
		conf.Duplicate = []map[string]string{{}}
	}

	for _, tagset := range conf.Duplicate {
		dcol := col
		dcol.Tags = map[string]string{}
		dcol.IndexSecondary = make([]int, downsampling.AggregatorCount)
		dcol.IndexPrimary = s.PrimaryCount
		s.PrimaryCount++

		for i, need := range needs {
			if need {
				dcol.IndexSecondary[i] = s.SecondaryCount
				s.SecondaryCount++
			}
		}

		for tag, value := range conf.Tags {
			dcol.Tags[tag] = value
		}

		for tag, value := range tagset {
			dcol.Tags[tag] = value
		}

		s.Columns = append(s.Columns, dcol)
	}

	return nil
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
		FlushCount:      conf.FlushCount,
		ForceFlushCount: conf.ForceFlushCount,
		FlushInterval:   conf.FlushInterval,
		LastFlush:       time.Now(),

		ReuseMax:    conf.ReuseMax,
		Columns:     make([]Column, 0),
		Tags:        conf.Tags,
		OldestValue: math.MaxInt64,

		Buckets: make([]Bucket, len(conf.Buckets)),

		Path: seriespath,

		PrimaryCount:   1,
		SecondaryCount: 2,
	}

	for _, colconf := range conf.Columns {
		err = s.addColumn(colconf)
		if err != nil {
			return Series{}, err
		}
		// todo: change this arbitrary limit to something meaningful
		if s.SecondaryCount > 127 {
			return Series{}, errors.New("column limit exceeded")
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

		s.Buckets[i], err = OpenBucket(s.Path, timeStep, conf.PointsFile)

		if i == 0 {
			s.Buckets[i].First = true
			s.Buckets[i].Buffer = storage.NewPointBuffer(s.PrimaryCount)
		} else {
			s.Buckets[i].Buffer = storage.NewPointBuffer(s.SecondaryCount)

		}

		if i == len(conf.Buckets)-1 {
			s.Buckets[i].Last = true
		} else {
			s.Buckets[i].Next = &s.Buckets[i+1]
		}

		if err != nil {
			return Series{}, err
		}
	}

	if err != nil {
		return Series{}, err
	}

	return s, nil
}

// CheckFlush checks if the series is due for a regular flush
func (s *Series) CheckFlush() bool {
	// never flush when all values were loaded from disk
	if s.OldestValue == math.MaxInt64 {
		return false
	}

	// flush if buffer size exceeds force flush count
	if s.Buckets[0].Buffer.Len() >= s.ForceFlushCount {
		return true
	}

	// don't do next check if the flush interval has not elapsed since the last flush
	if time.Now().Sub(s.LastFlush) < s.FlushInterval {
		return false
	}

	// check if buffer size exceeds flush count
	if s.Buckets[0].Buffer.Len() >= s.FlushCount {
		return true
	}

	return false
}

func (s *Series) Flush() {
	timeLimit := int64(math.MaxInt64)
	for i := range s.Buckets {
		// todo: may also force flush first bucket after flushinterval
		for s.Buckets[i].Flush(timeLimit, false) {
		}
		timeLimit = s.Buckets[i].LastTimeOnDisk
	}
}

// flush all values to disk, used to prepare for a shutdown
func (s *Series) FlushAll() {
	timeLimit := int64(math.MaxInt64)
	for i := range s.Buckets {
		for s.Buckets[i].Flush(timeLimit, i == 0) {
		}
		timeLimit = s.Buckets[i].LastTimeOnDisk
	}
}

func (s *Series) Query(columns []QueryColumn, timeRange TimeRange, timeStep int64) *Query {
	// find first bucket with timeStep smaller or equal to query
	i := len(s.Buckets) - 1
	for i > 0 {
		if s.Buckets[i].TimeStep <= timeStep {
			break
		}
		i--
	}

	return s.Buckets[i].Query(columns, timeRange, timeStep)
}
