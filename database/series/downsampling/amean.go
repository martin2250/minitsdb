package downsampling

type meanAggregator struct {
	index int
}

func (meanAggregator) GetIndex() int {
	return Mean.index
}

func (meanAggregator) Needs(indices []bool) {
	indices[Mean.index] = true
}

func (meanAggregator) AggregatePrimary(values []int64, times []int64) int64 {
	sum := Sum.AggregatePrimary(values, nil)
	count := Count.AggregatePrimary(values, nil)
	return sum / count
}

func (meanAggregator) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	var sum int64
	for i, v := range values[Mean.index] {
		sum += v * counts[i]
	}
	return sum / Sum.AggregatePrimary(counts, nil)
}
