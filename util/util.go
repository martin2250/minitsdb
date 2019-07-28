package util

import (
	"bytes"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// FileExists checks if file exists
func FileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

// RoundDown rounds down value to next multiple of modulo
// only works for positive integers
func RoundDown(value, modulo int64) int64 {
	if value < 0 {
		return 0
	}
	return (value / modulo) * modulo
}

// RoundUp rounds down value to next multiple of modulo
// only works for positive integers
func RoundUp(value, modulo int64) int64 {
	if value < 0 {
		return 0
	}
	return ((value + modulo - 1) / modulo) * modulo
}

// InsertFileBlock uses fallocate syscall to insert one or multiple empty blocks
// in the middle of a file, pushing back the contents that follow
func InsertFileBlock(file *os.File, offset, length int64) error {
	if length == 0 {
		return nil
	}

	return unix.Fallocate(int(file.Fd()), unix.FALLOC_FL_INSERT_RANGE, offset, length)
}

// IsSubset checks if map a is a subset of map b
func IsSubset(a map[string]string, b map[string]string) bool {
	for ka, va := range a {
		vb, ok := b[ka]
		if !ok || va != vb {
			return false
		}
	}
	return true
}

// ReadBlock reads a 4k block from a reader and returns it as bytes.Buffer
func ReadBlock(r io.Reader) (bytes.Buffer, error) {
	lr := io.LimitReader(r, 4096)
	var b bytes.Buffer

	n, err := b.ReadFrom(lr)

	if err != nil {
		return bytes.Buffer{}, err
	}

	if n == 0 {
		return bytes.Buffer{}, io.EOF
	}

	if n != 4096 {
		return bytes.Buffer{}, io.ErrUnexpectedEOF
	}

	return b, nil
}

// IndexOfInt64 returns the index of the first element of 'column' that matches 'element'
// returns -1 if no element matches
func IndexOfInt64(column []int64, element int64) int {
	for i, ct := range column {
		if element == ct {
			return i
		}
	}
	return -1
}
