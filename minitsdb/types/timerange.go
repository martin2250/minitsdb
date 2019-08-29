package types

import "github.com/martin2250/minitsdb/util"

type TimeRange struct {
	Start int64
	End   int64
}

func (r TimeRange) ContainsRange(other TimeRange) bool {
	return other.End <= r.End && other.Start >= r.Start
}
func (r TimeRange) Contains(time int64) bool {
	return r.Start <= time && time <= r.End
}

func (r TimeRange) Overlaps(other TimeRange) bool {
	return r.Contains(other.Start) || r.Contains(other.End) || other.ContainsRange(r)
}

func TimeRangeFromPoint(time, timeStep int64) TimeRange {
	x := util.RoundDown(time, timeStep)
	return TimeRange{
		Start: x,
		End:   x + timeStep - 1,
	}
}
