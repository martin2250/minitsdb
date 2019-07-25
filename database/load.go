package database

import (
	"errors"
	"io/ioutil"
	"path"

	"github.com/martin2250/minitsdb/database/series"
)

// NewDatabase creates a new database instance
func NewDatabase(databasePath string) (Database, error) {
	db := Database{
		Path:   databasePath,
		Series: make([]series.Series, 0),
	}

	err := db.loadSeries()

	return db, err
}

// LoadSeries loads all series in a database from the file system
func (db *Database) loadSeries() error {
	if len(db.Series) != 0 {
		return errors.New("series already loaded")
	}

	files, err := ioutil.ReadDir(db.Path)

	if err != nil {
		return err
	}

	for _, file := range files {
		if !file.IsDir() {
			continue
		}

		s, err := series.OpenSeries(path.Join(db.Path, file.Name()))

		if err != nil {
			return err
		}

		db.Series = append(db.Series, s)
	}

	return nil
}
