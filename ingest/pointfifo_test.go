package ingest

import (
	"math/rand"
	"runtime"
	"testing"
)

func addRandomPoint(pf *PointFifo) {
	point := Point{
		Time: int64(rand.Intn(30000) + 100),
		Tags: map[string]string{"k1": "v1"},
		Values: []Value{
			{
				Tags:  map[string]string{"k1": "v1"},
				Value: rand.ExpFloat64(),
			},
		},
	}

	pf.AddPoint(point)
}

// TestPointFifo mostly tests if fifo works when not using pointers
func TestPointFifo(t *testing.T) {
	pf := NewPointFifo()

	for i := 0; i < 100; i++ {
		addRandomPoint(&pf)
	}

	runtime.GC()

	for i := 0; i < 100; i++ {
		pointra, ok := pf.GetPoint()

		if !ok {
			t.Error("no value available")
		}

		if pointra.Time == 0 {
			t.Errorf("point invalid: %v", pointra)
		}

		t.Log(pointra)
	}

	_, ok := pf.GetPoint()

	if ok {
		t.Error("didn't expect more values")
	}
}
