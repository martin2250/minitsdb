package downsampling

type countFunction struct{}

func (countFunction) Needs(indices []bool) {

}

func (countFunction) AggregatePrimary(values []int64, times []int64) int64 {
	return int64(len(values))
}

func (countFunction) AggregateSecondary(values [][]int64, times []int64, counts []int64) int64 {
	return Sum.AggregatePrimary(counts, nil)
}
