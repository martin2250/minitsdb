package minitsdb

import (
	"errors"
	"io/ioutil"
	"path"
	"regexp"

	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/series"
)

// ServerSession holds the main application data
type ServerSession struct {
	Path   string
	Series []series.Series
}

// LoadDatabase creates a new server session, loading settings and database from the supplied path
func LoadDatabase(databasePath string) (ServerSession, error) {
	s := ServerSession{
		Path:   databasePath,
		Series: make([]series.Series, 0),
	}

	return s, nil
}

// LoadSeries loads all series from the file system
func (ss *ServerSession) LoadSeries() error {
	if len(ss.Series) != 0 {
		return errors.New("session already has series loaded")
	}

	files, err := ioutil.ReadDir(ss.Path)

	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		s, err := series.OpenSeries(path.Join(ss.Path, file.Name()))

		if err != nil {
			return err
		}

		ss.Series = append(ss.Series, s)
	}

	return nil
}

// FindSeries returns indices to all series that match the tagset
func (ss ServerSession) FindSeries(tags map[string]string) []int {
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
func (ss *ServerSession) InsertPoint(p ingest.Point) error {
	indices := ss.FindSeries(p.Tags)

	if len(indices) == 0 {
		return ErrSeriesUnknown
	}

	if len(indices) != 1 {
		return ErrSeriesAmbiguous
	}

	return ss.Series[indices[0]].InsertPoint(p)
}
