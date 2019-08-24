package downsampling

type maxAggregator struct {
	index int
}

func (maxAggregator) GetIndex() int {
	return Max.index
}

func (maxAggregator) Needs(indices []bool) {
	indices[Max.index] = true
}

func (maxAggregator) AggregatePrimary(values []int64, times []int64) int64 {
	max := values[0]
	for _, c := range values[1:] {
		if c > max {
			max = c
		}
	}
	return max
}

func (maxAggregator) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	return Max.AggregatePrimary(values[Max.index], nil)
}
