package storage

import (
	"encoding/binary"
	"github.com/jwilder/encoding/simple8b"
	"io"
)

// EncodeBlock encodes as many values into a 4k block as it can possibly fit
// returns the number of data points written
// if err == nil, exactly 4096 bytes were written to writer
func EncodeBlock(values [][]int64, transformers []Transformer, writer io.Writer) (int, error) {
	// check if counts match, else panic
	if len(values) != len(transformers) {
		panic("number of values and transformers don't match")
	}

	// tranform and encode all columns
	encoded := make([][]uint64, len(values))
	for i := range values {
		transformed, err := transformers[i].Apply(values[i])

		if err != nil {
			return 0, err
		}

		encoded[i], err = simple8b.EncodeAll(transformed)

		if err != nil {
			return 0, err
		}
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
				count, err := simple8b.Count(encoded[i][columns[i].words])

				if err != nil {
					return 0, err
				}

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
		if valuesTotal == len(values[0]) {
			break
		}
	}

	header := BlockHeader{
		BlockVersion: 1,
		NumPoints:    uint32(valuesTotal),
		NumColumns:   uint8(len(values)),
		TimeFirst:    values[0][0],
		TimeLast:     values[0][valuesTotal-1],
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
