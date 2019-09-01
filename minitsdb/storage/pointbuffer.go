package storage

import (
	"github.com/martin2250/minitsdb/util"
)

type PointBuffer struct {
	Values [][]int64
}

type Point struct {
	Values []int64
}

// InsertIndex returns the index where a point with the specified time
// can be inserted. if no index with this time exists, such an index is created
// time will be copied to this index, values must be copied by the caller
func (b *PointBuffer) InsertIndex(time int64) int {
	indexBuffer := 0
	var atEnd bool // value is appended to end of buffer

	for {
		if indexBuffer >= b.Len() {
			atEnd = true
			break
		}
		if b.Values[0][indexBuffer] == time {
			return indexBuffer
		}
		if b.Values[0][indexBuffer] > time {
			break
		}
		indexBuffer++
	}

	// in both cases the buffer needs to grow
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], 0)
	}

	if atEnd {
		b.Values[0][indexBuffer] = time
		return indexBuffer
	}

	// shift all values one back
	for i := range b.Values {
		copy(b.Values[i][indexBuffer+1:], b.Values[i][indexBuffer:])
	}

	b.Values[0][indexBuffer] = time
	return indexBuffer
}

// InsertPoint inserts a point into the buffer
// The index of the new point is determined automatically, new points can be inserted in the middle,
// replace an existing point or be appended to the start or end
// todo: test this
func (b *PointBuffer) InsertPoint(point Point) {
	if len(point.Values) != len(b.Values) {
		panic("value count mismatch")
	}

	indexBuffer := b.InsertIndex(point.Values[0])

	for i, val := range point.Values {
		b.Values[i][indexBuffer] = val
	}
}

// AppendPoint appends a point to the end of the buffer,
// without checking the timestamp of the point against the buffered values
func (b *PointBuffer) AppendPoint(point Point) {
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], point.Values[i])
	}
}

// AppendBuffer appends the points from another buffer to the buffer
func (b *PointBuffer) AppendBuffer(b2 PointBuffer) {
	for i := range b.Values {
		b.Values[i] = append(b.Values[i], b2.Values[i]...)
	}
}

// At returns the point at a specific index in the buffer
func (b PointBuffer) At(index int) Point {
	p := Point{
		Values: make([]int64, len(b.Values)),
	}
	for i := range b.Values {
		p.Values[i] = b.Values[i][index]
	}
	return p
}

// Len returns the number of points in the buffer
func (b PointBuffer) Len() int {
	return len(b.Values[0])
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

	for i := range b.Values {
		b.Values[i] = b.Values[i][n:]
	}
}

// Renew copies all buffers to new memory locations to allow the GC to clean up
func (b *PointBuffer) Renew() {
	b.Values = util.Copy2DInt64(b.Values)
}

// TrimStart discards all values that lie before start, assumes the buffer is sorted
func (b *PointBuffer) TrimStart(timeStart int64) {
	l := b.Len()
	s := 0
	for s < l && b.Values[0][s] < timeStart {
		s++
	}
	for i := range b.Values {
		b.Values[i] = b.Values[i][s:]
	}
}

// TrimEnd discards all values that lie after end, assumes the buffer is sorted
func (b *PointBuffer) TrimEnd(timeEnd int64) {
	e := b.Len()
	for e > 0 && b.Values[0][e-1] > timeEnd {
		e--
	}
	for j := range b.Values {
		b.Values[j] = b.Values[j][:e]
	}
}

// IndexOfTime returns the index to the first time that is greater or equal to the argument
// returns -1 if no time satisfies the condition
func (b *PointBuffer) IndexOfTime(time int64) int {
	index := 0
	for index < b.Len() {
		if b.Values[0][index] >= time {
			return index
		}
		index++
	}
	return -1
}

func NewPointBuffer(columns int) PointBuffer {
	out := PointBuffer{
		Values: make([][]int64, columns),
	}

	for i := range out.Values {
		out.Values[i] = make([]int64, 0)
	}

	return out
}
