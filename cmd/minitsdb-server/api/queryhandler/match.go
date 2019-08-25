package queryhandler

import (
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
)

func queriesFromDescription(db *minitsdb.Database, desc queryDescription) ([]Query, error) {
	var queries []Query

	// find matching series in the database
	for _, series := range db.FindSeries(desc.Series, true) {
		query := Query{
			Series: series,
		}

		if len(desc.Columns) == 0 {
			// add all columns
			for i := range series.Columns {
				query.Columns = append(query.Columns, minitsdb.QueryColumn{
					Column:   &series.Columns[i],
					Function: series.Columns[i].DefaultFunction,
				})
			}
		} else {
			// add matching columns for each colspec
			for _, colspec := range desc.Columns {
				for _, column := range series.FindColumns(colspec.Tags, true) {
					qc := minitsdb.QueryColumn{
						Column: column,
					}

					if colspec.Function == "" {
						qc.Function = column.DefaultFunction
					} else {
						var err error
						qc.Function, err = downsampling.FindFunction(colspec.Function)
						if err != nil {
							return nil, err
						}
					}

					if column.Supports(qc.Function) {
						query.Columns = append(query.Columns, qc)
					}
				}
			}
		}
	}

	return queries, nil
}
