package ingest

import (
	"container/list"
)

// PointFifo is a simple fifo queue for points with thread-safe access
type PointList struct {
	fifo *list.List
}

// NewPointFifo initializes a PointFifo struct
func NewPointList() PointList {
	return PointList{
		fifo: list.New(),
	}
}

// GetPoint pops a point from the fifo. thread safe
func (pf PointList) GetPoint() (Point, bool) {
	pointi := pf.fifo.Front()

	if pointi == nil {
		return Point{}, false
	}

	pf.fifo.Remove(pointi)

	point, ok := pointi.Value.(*Point)

	if !ok {
		return Point{}, false
	}

	p := *point

	point = nil

	return p, true
}

// AddPoint pushes a point onto the fifo
func (pf PointList) AddPoint(point Point) {
	pf.fifo.PushBack(&point)
}
