package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	. "github.com/martin2250/minitsdb/minitsdb/types"
)

func DownsampleQuery(src [][]int64, queryColumns []QueryColumn, timeStep int64, force bool, indexStart *int, primary bool) storage.PointBuffer {
	// create output array
	output := storage.PointBuffer{
		Time:   make([]int64, 0),
		Values: make([][]int64, len(queryColumns)),
	}

	length := len(src[0])

	var srcColSecondary [][]int64
	if !primary {
		srcColSecondary = make([][]int64, downsampling.AggregatorCount)
	}

	for *indexStart < length {
		indexEnd := -1
		currentRange := TimeRangeFromPoint(src[0][*indexStart], timeStep)

		for i := *indexStart; i < length; i++ {
			if src[0][i] == currentRange.End {
				indexEnd = i + 1
				break
			}
			if src[0][i] > currentRange.End {
				indexEnd = i
				break
			}
		}

		// not enough values to fill this time step
		if indexEnd == -1 && !force {
			break
		}

		output.Time = append(output.Time, currentRange.Start)

		for i, qc := range queryColumns {
			var val int64
			if primary {
				val = qc.Function.AggregatePrimary(src[qc.Column.IndexPrimary][*indexStart:indexEnd], src[0][*indexStart:indexEnd])
			} else {
				for i, index := range qc.Column.IndexSecondary {
					if index > 1 {
						srcColSecondary[i] = src[index][*indexStart:indexEnd]
					} else {
						srcColSecondary[i] = nil
					}
				}
				val = qc.Function.AggregateSecondary(srcColSecondary, src[0][*indexStart:indexEnd], src[1][*indexStart:indexEnd])
			}
			output.Values[i] = append(output.Values[i], val)
		}

		*indexStart = indexEnd
	}

	return output
}
