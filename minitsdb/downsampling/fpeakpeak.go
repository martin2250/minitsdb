package downsampling

type peakpeakFunction struct{}

func (peakpeakFunction) Needs(indices []bool) {
	indices[Max.index] = true
	indices[Min.index] = true
}

func (peakpeakFunction) AggregatePrimary(values []int64, times []int64) int64 {
	max := Max.AggregatePrimary(values, nil)
	min := Min.AggregatePrimary(values, nil)
	return max - min
}

func (peakpeakFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	max := Max.AggregateSecondary(values, nil, nil)
	min := Min.AggregateSecondary(values, nil, nil)
	return max - min
}
