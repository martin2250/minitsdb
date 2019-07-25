package database

import (
	"errors"
	"regexp"

	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/ingest"
)

// Database holds series from a database directory
type Database struct {
	Path   string
	Series []series.Series
}

// FindSeries returns indices to all series that match the tagset
func (ss Database) FindSeries(tags map[string]string) []int {
	indices := make([]int, 0)

	for i, series := range ss.Series {
		matches := true

		for queryKey, queryValue := range tags {
			seriesValue, ok := series.Tags[queryKey]
			if !ok {
				matches = false
				break
			}

			ok, _ = regexp.MatchString(queryValue, seriesValue)
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

// ErrSeriesAmbiguous indicates that the insert failed because point value tags match two series
var ErrSeriesAmbiguous = errors.New("point values matches two series")

// ErrSeriesUnknown indicates that the insert failed because one of the values could not be assigned to a series
var ErrSeriesUnknown = errors.New("value doesn't match any series")

// InsertPoint finds a matching series and tries to insert the point
func (ss *Database) InsertPoint(p ingest.Point) error {
	indices := ss.FindSeries(p.Tags)

	if len(indices) == 0 {
		return ErrSeriesUnknown
	}

	if len(indices) != 1 {
		return ErrSeriesAmbiguous
	}

	return ss.Series[indices[0]].InsertPoint(p)
}
