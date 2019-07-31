package database

import "github.com/martin2250/minitsdb/util"

type PointBuffer struct {
	Time   []int64
	Values [][]int64
}

type Point struct {
	Time   int64
	Values []int64
}

// todo: test this
func (b *PointBuffer) InsertPoint(point Point) {
	indexBuffer := 0
	var insert bool // value is inserted into the buffer, pushing part of the buffer back
	var atEnd bool  // value is appended to end of buffer

	for {
		if b.Time[indexBuffer] == point.Time {
			break
		}
		if b.Time[indexBuffer] > point.Time {
			insert = true
			break
		}
		if indexBuffer >= len(b.Time) {
			atEnd = true
			break
		}
		indexBuffer++
	}

	if atEnd || insert {
		// in both cases the buffer needs to grow
		b.AppendPoint(point)

		if atEnd {
			return
		}

		// shift all values one back
		copy(b.Time[indexBuffer+1:], b.Time[indexBuffer:])
		for i := range point.Values {
			copy(b.Values[i][indexBuffer+1:], b.Values[i][indexBuffer:])
		}
	}

	b.Time[indexBuffer] = point.Time
	for i, val := range point.Values {
		b.Values[i][indexBuffer] = val
	}
}

func (b *PointBuffer) AppendPoint(point Point) {
	b.Time = append(b.Time, point.Time)
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], point.Values[i])
	}
}

func (b *PointBuffer) AppendBuffer(b2 PointBuffer) {
	b.Time = append(b.Time, b2.Time...)
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], b2.Values[i]...)
	}
}

func (b PointBuffer) At(index int) Point {
	p := Point{
		Time:   b.Time[index],
		Values: make([]int64, len(b.Values)),
	}
	for i := range b.Values {
		p.Values[i] = b.Values[index][i]
	}
	return p
}

func (b PointBuffer) Len() int {
	return len(b.Time)
}

// Renew copies all buffers to new memory locations to allow the GC to clean up
func (b *PointBuffer) Renew() {
	b.Time = util.Copy1DInt64(b.Time)
	b.Values = util.Copy2DInt64(b.Values)
}
