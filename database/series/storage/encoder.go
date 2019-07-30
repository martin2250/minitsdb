package storage

import (
	"encoding/binary"
	"github.com/jwilder/encoding/simple8b"
	"io"
)

// EncodeBlock encodes as many values into a 4k block as it can possibly fit
// returns the number of data points written
// if err == nil, exactly 4096 bytes were written to writer
// values must have 255 or fewer entries
// times is only used to fill the block header TimeFirst and TimeLast, must also be stored in values (in transformed form)
func EncodeBlock(writer io.Writer, times []int64, values [][]uint64) (int, error) {
	valuesAvailable := len(times)
	encoded := make([][]uint64, len(values))

	for i, val := range values {
		if len(val) != valuesAvailable {
			panic("input slices have different lengths")
		}
		// pre-allocate some words to make appends faster
		encoded[i] = make([]uint64, 64)[:0]
	}

	// try to fit as many values into 512 words as possible
	// used to keep track of how many encoded words each column needs so all have the same number of values
	columns := make([]struct {
		words     int // number of words (confirmed to fit in last loop)
		wordsNext int // number of words (tested in current loop iteration)
		values    int // number of values contained
	}, len(values))

	valuesTotal := 0
	wordsTotal := 3 // 3 words occupied by header

	// increase numbers of values until total number of words exceeds block size
	for {
		// increase valuesTotal and check if this number of values still fits
		valuesTotal++
		// total number of words after this round, checked to be smaller
		// than wordsMax at the end of the loop iteration
		wordsTotalNext := wordsTotal
		// give every column with less values one more word
		for i := range columns {
			if columns[i].values < valuesTotal {
				// count number of values in the next word
				encodedWord, count, err := simple8b.Encode(values[i][columns[i].values:])

				if err != nil {
					return 0, err
				}

				encoded[i] = append(encoded[i], encodedWord)

				wordsTotalNext++
				columns[i].wordsNext++
				columns[i].values += count
			}
		}

		// valuesTotal exceeds block capacity
		// do not update columns.words
		// use last valid number of values
		if wordsTotalNext > 512 {
			valuesTotal--
			break
		}

		// this number of words still fits into the block
		// update number of words allocated to each column
		for i := range columns {
			columns[i].words = columns[i].wordsNext
		}
		wordsTotal = wordsTotalNext

		// no more values left to store
		if valuesTotal == valuesAvailable {
			break
		}
	}

	header := BlockHeader{
		BlockVersion: 1,
		NumPoints:    uint32(valuesTotal),
		NumColumns:   uint8(len(values)),
		TimeFirst:    times[0],
		TimeLast:     times[valuesTotal-1],
		BytesUsed:    uint16(8 * wordsTotal),
	}

	// write header
	if err := binary.Write(writer, binary.LittleEndian, header); err != nil {
		return 0, err
	}

	// write columns
	for i := range columns {
		if err := binary.Write(writer, binary.LittleEndian, encoded[i][:columns[i].words]); err != nil {
			return 0, err
		}
	}

	// ensure that 512 words are written
	if _, err := writer.Write(make([]uint8, 8*(512-wordsTotal))); err != nil {
		return 0, err
	}

	return valuesTotal, nil
}

func EncodeAll(writer io.Writer, times []int64, values [][]uint64) error {
	// create a shallow copy of input arrays, so they are not modified
	timesCopy := make([]int64, len(times))
	valuesCopy := make([][]uint64, len(values))
	for i, val := range values {
		valuesCopy[i] = val
	}

	n := len(times)
	for n > 0 {
		c, err := EncodeBlock(writer, timesCopy, valuesCopy)
		if err != nil {
			return err
		}
		timesCopy = timesCopy[c:]
		for i := range valuesCopy {
			valuesCopy[i] = valuesCopy[i][c:]
		}
		n -= c
	}
	return nil
}
