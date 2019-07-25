package ingest

import (
	"io"
	"math/rand"
	"runtime"
	"testing"
)

func addRandomPoint(pf *PointFifo) {
	point := Point{
		Time: int64(rand.Intn(30000) + 100),
		Tags: map[string]string{"k1": "v1"},
		Values: []Value{
			Value{
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
		pointra, err := pf.GetPoint()

		if err != nil {
			t.Errorf("error: %v", err)
		}

		if pointra.Time == 0 {
			t.Errorf("point invalid: %v", pointra)
		}

		t.Log(pointra)
	}

	_, err := pf.GetPoint()

	if err != io.EOF {
		t.Errorf("error should be io.EOF: %v", err)
	}
}
