package queryhandler

import (
	"errors"
	"github.com/martin2250/minitsdb/util"
	"gopkg.in/yaml.v3"
	"io"
	"time"
)

type queryDescription struct {
	Series  map[string]string
	Columns []struct {
		Tags     map[string]string
		Function string
		Factor   *float64
	}
	TimeStep  string
	timeStep  time.Duration // todo: replace this with a prettier solution
	TimeStart int64
	TimeEnd   int64
	Wait      bool
	Text      bool
}

func parseQuery(r io.Reader) (queryDescription, error) {
	desc := queryDescription{}
	{
		dec := yaml.NewDecoder(r)
		dec.KnownFields(true)
		err := dec.Decode(&desc)

		if err != nil {
			return queryDescription{}, err
		}
	}

	if desc.Series == nil {
		return queryDescription{}, errors.New("no series tags specified")
	}

	for _, c := range desc.Columns {
		if c.Tags == nil {
			return queryDescription{}, errors.New("no column tags specified")
		}
	}

	var err error
	desc.timeStep, err = util.ParseDuration(desc.TimeStep)

	if err != nil {
		return queryDescription{}, err
	}

	if desc.timeStep < 1*time.Second {
		desc.timeStep = 1 * time.Second
	}

	if desc.TimeEnd <= desc.TimeStart {
		return queryDescription{}, errors.New("invalid time range")
	}

	return desc, nil
}
