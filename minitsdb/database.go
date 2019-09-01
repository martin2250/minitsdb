package minitsdb

import (
	"errors"
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

// ErrSeriesAmbiguous indicates that the insert failed because point value tags match two series
var ErrSeriesAmbiguous = errors.New("point values matches two series")

// ErrSeriesUnknown indicates that the insert failed because one of the values could not be assigned to a series
var ErrSeriesUnknown = errors.New("value doesn't match any series")

func (db *Database) FlushSeries() {
	for _, s := range db.Series {
		if s.CheckFlush() {
			s.Flush()
		}
	}
}
