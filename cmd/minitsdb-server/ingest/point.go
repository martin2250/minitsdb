package ingest

// Value holds a value
type Value struct {
	Tags  map[string]string
	Value float64
}

// Point holds information about a point to be inserted into the series
type Point struct {
	Tags   map[string]string
	Time   int64
	Values []Value
}

// PointSink can store incoming points for later ingestion by a database
type PointSink interface {
	// AddPoint stores a point in the sink
	AddPoint(point Point)
}

type ChanPointSink chan<- Point

func (cps ChanPointSink) AddPoint(point Point) {
	cps <- point
}

// PointSource returns points that should be stored in the database
// implementations might include a simple fifo or a rpc call to a
// separate process that handles and buffers ingests
type PointSource interface {
	// GetPoint returns a point to be inserted into the database
	// the point is deleted from the source
	// returns false if no point is available
	GetPoint() (Point, bool)
}
