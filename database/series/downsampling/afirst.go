package downsampling

type firstAggregator struct {
	index int
}

func (firstAggregator) GetIndex() int {
	return First.index
}

func (firstAggregator) Needs(indices []bool) {
	indices[First.index] = true
}

func (firstAggregator) AggregatePrimary(values []int64, times []int64) int64 {
	return values[0]
}

func (firstAggregator) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	return values[First.index][0]
}
