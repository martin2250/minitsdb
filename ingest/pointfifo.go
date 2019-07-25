package ingest

import (
	"io"

	fifo "github.com/foize/go.fifo"
)

// PointFifo is a simple fifo queue for points with thread-safe access
type PointFifo struct {
	fifo *fifo.Queue
}

// NewPointFifo initializes a PointFifo struct
func NewPointFifo() PointFifo {
	return PointFifo{
		fifo: fifo.NewQueue(),
	}
}

// GetPoint pops a point from the fifo. thread safe
func (pf PointFifo) GetPoint() (Point, error) {
	pointi := pf.fifo.Next()

	if pointi == nil {
		return Point{}, io.EOF
	}

	point, ok := pointi.(Point)

	if !ok {
		return Point{}, io.EOF
	}

	return point, nil
}

// AddPoint pushes a point onto the fifo
func (pf PointFifo) AddPoint(point Point) {
	pf.fifo.Add(point)
}
