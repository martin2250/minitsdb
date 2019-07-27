package series

type Aggregator interface {
	Aggregate() int64
}
