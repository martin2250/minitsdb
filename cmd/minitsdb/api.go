package main

import (
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/ingest"
)

func associatePoints(input <-chan ingest.Point, output chan<- series.AssociatedPoint, db *database.Database) {
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

		output <- series.AssociatedPoint{
			Point:  ps,
			Series: indices[0],
		}
	}
}
