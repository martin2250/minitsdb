package main

import (
	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/minitsdb"
)

func associatePoints(input <-chan ingest.Point, output chan<- minitsdb.AssociatedPoint, db *minitsdb.Database) {
	for {
		p := <-input

		indices := db.FindSeries(p.Tags)
		if len(indices) != 1 {
			continue
		}

		ps, err := indices[0].ConvertPoint(p)
		if err != nil {
			continue
		}

		output <- minitsdb.AssociatedPoint{
			Point:  ps,
			Series: indices[0],
		}
	}
}
