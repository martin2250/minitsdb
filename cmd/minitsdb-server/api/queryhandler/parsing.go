package queryhandler

import (
	"errors"
	"gopkg.in/yaml.v3"
	"io"
	"time"
)

type queryDescription struct {
	Series  map[string]string
	Columns []struct {
		Tags     map[string]string
		Function string
	}
	TimeStep  time.Duration
	TimeStart int64
	TimeEnd   int64
	Wait      bool
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

	if desc.TimeStep < 1*time.Second {
		return queryDescription{}, errors.New("time step smaller than 1s")
	}

	if desc.TimeEnd <= desc.TimeStart {
		return queryDescription{}, errors.New("invalid time range")
	}

	return desc, nil
}
