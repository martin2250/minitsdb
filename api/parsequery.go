package api

import (
	"errors"
	"io"

	"gopkg.in/yaml.v2"
)

type yamlQueryParameters struct {
	Series  map[string]string
	Columns []map[string]string

	TimeFrom int64
	TimeTo   int64

	// LimitSeries is the maximum number of series returned
	LimitSeries int

	// Resolution is the smallest allowed time step between points, either Points or Resolution must be set
	Resolution int
	// Points is the maximum number of points returned, either Points or Resolution must be set
	Points int
}

func parseQuery(r io.Reader) (yamlQueryParameters, error) {
	d := yaml.NewDecoder(r)
	d.SetStrict(true)

	c := yamlQueryParameters{
		LimitSeries: 1000,
		Resolution:  -1,
		Points:      -1,
	}

	err := d.Decode(&c)

	if err != nil {
		return yamlQueryParameters{}, err
	}

	err = c.Check()

	if err != nil {
		return yamlQueryParameters{}, err
	}

	return c, nil
}

func (p yamlQueryParameters) Check() error {
	if p.Series == nil || len(p.Series) < 1 {
		return errors.New("series description missing")
	}

	if p.Columns == nil || len(p.Columns) < 1 {
		return errors.New("column description missing")
	}

	for _, col := range p.Columns {
		if col == nil || len(col) < 1 {
			return errors.New("column description incomplete")
		}
	}

	if p.LimitSeries < 1 {
		return errors.New("series limit out of range")
	}

	if p.Resolution < -1 || p.Resolution == 0 {
		return errors.New("resolution invalid")
	}

	if p.Points < -1 || p.Points == 0 {
		return errors.New("points invalid")
	}

	if (p.Points == -1) == (p.Resolution == -1) {
		return errors.New("either points or resolution, not both, must be provided")
	}

	return nil
}
