package downsampling

type minAggregator struct {
	index int
}

func (minAggregator) GetIndex() int {
	return Min.index
}

func (minAggregator) Needs(indices []bool) {
	indices[Min.index] = true
}

func (minAggregator) AggregatePrimary(values []int64, times []int64) int64 {
	min := values[0]
	for _, c := range values[1:] {
		if c < min {
			min = c
		}
	}
	return min
}

func (minAggregator) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	return Min.AggregatePrimary(values[Min.index], nil)
}
