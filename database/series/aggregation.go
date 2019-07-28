package series

type Aggregator interface {
	AggregateFirst(i []int64) int64
}
