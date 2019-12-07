package queryhandler

import (
	"github.com/martin2250/minitsdb/minitsdb"
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
)

func queriesFromDescription(db *minitsdb.Database, desc queryDescription) ([]*SubQuery, error) {
	var queries []*SubQuery

	// find matching series in the database
	for _, series := range db.FindSeries(desc.Series, true) {
		query := SubQuery{
			Series: series,
		}

		if len(desc.Columns) == 0 {
			// add all columns
			for i := range series.Columns {
				query.Columns = append(query.Columns, minitsdb.QueryColumn{
					Column:   &series.Columns[i],
					Function: series.Columns[i].DefaultFunction,
					Factor:   1.0,
				})
			}
		} else {
			// add matching columns for each colspec
			for _, colspec := range desc.Columns {
				for _, column := range series.FindColumns(colspec.Tags, true) {
					qc := minitsdb.QueryColumn{
						Column: column,
						Factor: 1.0,
					}

					if colspec.Factor != nil {
						qc.Factor = *colspec.Factor
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
		if len(query.Columns) != 0 {
			queries = append(queries, &query)
		}
	}

	return queries, nil
}
