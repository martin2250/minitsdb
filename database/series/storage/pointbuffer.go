package storage

import "github.com/martin2250/minitsdb/util"

type PointBuffer struct {
	Time   []int64
	Values [][]int64
}

type Point struct {
	Time   int64
	Values []int64
}

// InsertPoint inserts a point into the buffer
// The index of the new point is determined automatically, new points can be inserted in the middle,
// replace an existing point or be appended to the start or end
// todo: test this
func (b *PointBuffer) InsertPoint(point Point) {
	indexBuffer := 0
	var insert bool // value is inserted into the buffer, pushing part of the buffer back
	var atEnd bool  // value is appended to end of buffer

	for {
		if indexBuffer >= len(b.Time) {
			atEnd = true
			break
		}
		if b.Time[indexBuffer] == point.Time {
			break
		}
		if b.Time[indexBuffer] > point.Time {
			insert = true
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

// AppendPoint appends a point to the end of the buffer,
// without checking the timestamp of the point against the buffered values
func (b *PointBuffer) AppendPoint(point Point) {
	b.Time = append(b.Time, point.Time)
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], point.Values[i])
	}
}

// AppendBuffer appends the points from another buffer to the buffer
func (b *PointBuffer) AppendBuffer(b2 PointBuffer) {
	b.Time = append(b.Time, b2.Time...)
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], b2.Values[i]...)
	}
}

// At returns the point at a specific index in the buffer
func (b PointBuffer) At(index int) Point {
	p := Point{
		Time:   b.Time[index],
		Values: make([]int64, len(b.Values)),
	}
	for i := range b.Values {
		p.Values[i] = b.Values[i][index]
	}
	return p
}

// Len returns the number of points in the buffer
func (b PointBuffer) Len() int {
	return len(b.Time)
}

// Len returns the number of columns in the buffer
func (b PointBuffer) Cols() int {
	return len(b.Values)
}

// Discard first n points from buffer
func (b *PointBuffer) Discard(n int) {
	if n > b.Len() {
		n = b.Len()
	}

	b.Time = b.Time[n:]
	for i := range b.Values {
		b.Values[i] = b.Values[i][n:]
	}
}

// Renew copies all buffers to new memory locations to allow the GC to clean up
func (b *PointBuffer) Renew() {
	b.Time = util.Copy1DInt64(b.Time)
	b.Values = util.Copy2DInt64(b.Values)
}

func NewPointBuffer(columns int) PointBuffer {
	out := PointBuffer{
		Time:   make([]int64, 0),
		Values: make([][]int64, columns),
	}

	for i := range out.Values {
		out.Values[i] = make([]int64, 0)
	}

	return out
}
