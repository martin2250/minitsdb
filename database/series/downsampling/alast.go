package downsampling

type lastAggregator struct {
	index int
}

func (lastAggregator) GetIndex() int {
	return Last.index
}

func (lastAggregator) Needs(indices []bool) {
	indices[Last.index] = true
}

func (lastAggregator) AggregatePrimary(values []int64, times []int64) int64 {
	return values[len(values)-1]
}

func (lastAggregator) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	return values[Last.index][len(values[Last.index])-1]
}
