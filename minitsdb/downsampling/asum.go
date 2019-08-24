package downsampling

type sumAggregator struct {
	index int
}

func (sumAggregator) GetIndex() int {
	return Sum.index
}

func (sumAggregator) Needs(indices []bool) {
	indices[Sum.index] = true
}

func (sumAggregator) AggregatePrimary(values []int64, times []int64) int64 {
	var sum int64
	for _, c := range values {
		sum += c
	}
	return sum
}

func (sumAggregator) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	return Sum.AggregatePrimary(values[Sum.index], nil)
}
