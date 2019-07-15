package series

import (
	"time"
)

// Series describes a time series, id'd by a name and tags
type Series struct {
	Values        [][]int64
	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Path          string
	Columns       []Column
	Buckets       []Bucket

	Tags       map[string]string
	FlushDelay time.Duration
	BufferSize int
	ReuseMax   int
}

// Column holds the json structure that describes a column in a series
type Column struct {
	Tags     map[string]string
	Decimals int
}

// OpenSeries opens series from file
func OpenSeries(seriespath string) (Series, error) {
	conf, err := LoadSeriesYamlConfig(seriespath)

	if err != nil {
		return Series{}, err
	}

	s := Series{
		FlushDelay: conf.FlushDelay,
		BufferSize: conf.Buffer,
		ReuseMax:   conf.ReuseMax,
		Columns:    make([]Column, 0),
		Buckets:    make([]Bucket, len(conf.Buckets)),
		Tags:       conf.Tags,

		Path: seriespath,
	}

	timeStep := int64(1)

	for i, bc := range conf.Buckets {
		timeStep *= int64(bc.Factor)

		s.Buckets[i].series = &s
		s.Buckets[i].TimeStep = timeStep

		s.Buckets[i].First = (i == 0)
	}

	for _, colconf := range conf.Columns {
		if colconf.Duplicate == nil {
			s.Columns = append(s.Columns, Column{
				Decimals: colconf.Decimals,
				Tags:     colconf.Tags,
			})
		} else {
			for _, tagset := range colconf.Duplicate {
				col := Column{
					Decimals: colconf.Decimals,
					Tags:     make(map[string]string),
				}

				for tag, value := range colconf.Tags {
					col.Tags[tag] = value
				}

				for tag, value := range tagset {
					col.Tags[tag] = value
				}
				s.Columns = append(s.Columns, col)
			}
		}
	}

	s.Values = make([][]int64, 1+len(s.Columns))

	for i := range s.Values {
		s.Values[i] = make([]int64, 0)
	}

	return s, nil
}
