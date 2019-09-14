package minitsdb

import (
	"errors"
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
)

func DownsamplePoint(src storage.PointBuffer, columns []QueryColumn, timeRange TimeRange, primary bool) (storage.Point, error) {
	indexStart := src.IndexOfTime(timeRange.Start)

	if indexStart == -1 {
		return storage.Point{}, errors.New("no points found")
	}

	if src.Values[0][indexStart] > timeRange.End {
		return storage.Point{}, errors.New("no points found")
	}

	indexEnd := indexStart + 1
	for indexEnd < src.Len() {
		if src.Values[0][indexEnd] > timeRange.End {
			break
		}
		indexEnd++
	}

	p := storage.Point{
		Values: make([]int64, 1+len(columns)),
	}

	p.Values[0] = timeRange.Start

	for i, qc := range columns {
		var val int64
		if primary {
			val = qc.Function.AggregatePrimary(src.Values[qc.Column.IndexPrimary][indexStart:indexEnd], src.Values[0][indexStart:indexEnd])
		} else {
			srcColSecondary := make([][]int64, downsampling.AggregatorCount)
			for i, index := range qc.Column.IndexSecondary {
				if index > 1 {
					srcColSecondary[i] = src.Values[index][indexStart:indexEnd]
				}
			}
			val = qc.Function.AggregateSecondary(srcColSecondary, src.Values[0][indexStart:indexEnd], src.Values[1][indexStart:indexEnd])
		}
		p.Values[i+1] = val
	}

	return p, nil
}

func DownsampleQuery(src storage.PointBuffer, queryColumns []QueryColumn, timeStep int64, force bool, indexStart *int, primary bool) storage.PointBuffer {
	// create output array
	output := storage.PointBuffer{
		Values: make([][]int64, 1+len(queryColumns)),
	}

	length := src.Len()

	var srcColSecondary [][]int64
	if !primary {
		srcColSecondary = make([][]int64, downsampling.AggregatorCount)
	}

	for *indexStart < length {
		indexEnd := -1
		// todo: calculate currentRange.End with input time step in mind
		currentRange := TimeRangeFromPoint(src.Values[0][*indexStart], timeStep)

		for i := *indexStart; i < length; i++ {
			if src.Values[0][i] == currentRange.End {
				indexEnd = i + 1
				break
			}
			if src.Values[0][i] > currentRange.End {
				indexEnd = i
				break
			}
		}

		// not enough values to fill this time step
		if indexEnd == -1 {
			if !force {
				break
			}
			indexEnd = length
		}

		output.Values[0] = append(output.Values[0], currentRange.Start)

		for i, qc := range queryColumns {
			var val int64
			if primary {
				val = qc.Function.AggregatePrimary(src.Values[qc.Column.IndexPrimary][*indexStart:indexEnd], src.Values[0][*indexStart:indexEnd])
			} else {
				for i, index := range qc.Column.IndexSecondary {
					if index > 1 && src.Need[index] {
						srcColSecondary[i] = src.Values[index][*indexStart:indexEnd]
					} else {
						srcColSecondary[i] = nil
					}
				}
				val = qc.Function.AggregateSecondary(srcColSecondary, src.Values[0][*indexStart:indexEnd], src.Values[1][*indexStart:indexEnd])
			}
			output.Values[i+1] = append(output.Values[i+1], val)
		}

		*indexStart = indexEnd
	}

	return output
}
