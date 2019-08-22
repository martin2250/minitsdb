package database

import (
	"errors"
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/ingest"
)

// Database holds series from a database directory
type Database struct {
	Path   string
	Series []series.Series
}

// FindSeries returns indices to all series that match the tagset
func (db Database) FindSeries(tags map[string]string) []*series.Series {
	matches := make([]*series.Series, 0)

	for i, series := range db.Series {
		isMatch := true

		for queryKey, queryValue := range tags {
			seriesValue, ok := series.Tags[queryKey]
			if !ok {
				isMatch = false
				break
			}

			//ok, _ = regexp.MatchString(queryValue, seriesValue)
			ok = queryValue == seriesValue
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

// InsertPoint finds a matching series and tries to insert the point
// todo: move this somewhere else, db is only used once
func (db *Database) InsertPoint(p ingest.Point) error {
	indices := db.FindSeries(p.Tags)

	if len(indices) == 0 {
		return ErrSeriesUnknown
	}

	if len(indices) != 1 {
		return ErrSeriesAmbiguous
	}

	ps, err := indices[0].ConvertPoint(p)

	if err != nil {
		return err
	}

	err = indices[0].InsertPoint(ps)

	if indices[0].CheckFlush() {
		indices[0].Flush()
	}

	if err != nil {
		return err
	}

	return nil
}

func (db *Database) FlushSeries() {
	for _, s := range db.Series {
		if s.CheckFlush() {
			s.Flush()
		}
	}
}
