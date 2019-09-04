package minitsdb

import (
	"errors"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"math"
	"regexp"
	"strings"
)

// Database holds series from a database directory
type Database struct {
	Path   string
	Series []Series
}

// FindSeries finds all series that match the given set of tags
// if useRegex is true, all tag values of format /.../ as treated as regexes
func (db Database) FindSeries(tags map[string]string, useRegex bool) []*Series {
	matches := make([]*Series, 0)

	for i, series := range db.Series {
		isMatch := true

		for queryKey, queryValue := range tags {
			seriesValue, ok := series.Tags[queryKey]
			if !ok {
				isMatch = false
				break
			}

			if useRegex && strings.HasPrefix(queryValue, "/") && strings.HasSuffix(queryValue, "/") {
				ok, _ = regexp.MatchString(queryValue[1:len(queryValue)-1], seriesValue)
			} else {
				ok = queryValue == seriesValue
			}

			if !ok {
				isMatch = false
				break
			}
		}

		if isMatch {
			matches = append(matches, &db.Series[i])
		}
	}

	return matches
}

func (db *Database) FlushSeries() {
	for _, s := range db.Series {
		if s.CheckFlush() {
			s.Flush()
		}
	}
}

var ErrSeriesAmbiguous = errors.New("series tags ambiguous")
var ErrSeriesUnknown = errors.New("no matching series found")
var ErrColumnsCount = errors.New("point has wrong number of values")
var ErrColumnUnknown = errors.New("no matching column found")

func (db *Database) AssociatePoint(point lineprotocol.Point) (*Series, storage.Point, error) {
	var s *Series

	for i := range db.Series {
		if lineprotocol.MatchKVPs(point.Series, db.Series[i].Tags) {
			if s == nil {
				s = &db.Series[i]
			} else {
				return nil, storage.Point{}, ErrSeriesAmbiguous
			}
		}
	}

	if s == nil {
		return nil, storage.Point{}, ErrSeriesUnknown
	}

	if len(point.Values) != len(s.Columns) {
		return nil, storage.Point{}, ErrColumnsCount
	}

	values := make([]int64, s.PrimaryCount)
	filled := make([]bool, s.PrimaryCount)

	values[0] = point.Time

	for _, v := range point.Values {
		var c *Column

		for i := range s.Columns {
			if !filled[s.Columns[i].IndexPrimary] && lineprotocol.MatchKVPs(v.Tags, s.Columns[i].Tags) {
				c = &s.Columns[i]
				break // don't need to check for ambiguity as number of values must match
			}
		}

		if c == nil {
			return nil, storage.Point{}, ErrColumnUnknown
		}

		filled[c.IndexPrimary] = true
		values[c.IndexPrimary] = int64(math.Round(v.Value * math.Pow10(c.Decimals)))
	}

	return s, storage.Point{Values: values}, nil
}

func (db *Database) Downsample() {
	for is := range db.Series {
		for ib := range db.Series[is].Buckets {
			if !db.Series[is].Buckets[ib].Downsample() {
				break
			}
		}
	}
}
