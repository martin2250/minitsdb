package downsampling

type differenceFunction struct {
}

func (differenceFunction) Needs(indices []bool) {
	Last.Needs(indices)
	First.Needs(indices)
}

func (differenceFunction) AggregatePrimary(values []int64, times []int64) int64 {
	last := Last.AggregatePrimary(values, nil)
	first := First.AggregatePrimary(values, nil)
	return last - first
}

func (differenceFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	last := Last.AggregateSecondary(values, nil, nil)
	first := First.AggregateSecondary(values, nil, nil)
	return last - first
}
