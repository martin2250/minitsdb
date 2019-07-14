package minitsdb

import (
	"errors"
	"io/ioutil"
	"path"

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
