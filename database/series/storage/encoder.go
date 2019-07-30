package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/jwilder/encoding/simple8b"
	"io"
)

type Encoder struct {
	writer io.Writer
	// Columns contains a list of columns that should be encoded
	// must be sorted by Index
	Transformers []Transformer
}

// EncodeBlock encodes as many values into a 4k block as it can possibly fit
// returns the binary block and the number of data points contained within
func EncodeBlock(values [][]int64) (bytes.Buffer, int, error) {
	encoded := make([][]uint64, len(values))
	var buffer bytes.Buffer

	for i := range values {
		var err error

		//encoded[i], err = simple8b.EncodeAll(doTransform(values[i]))
		fmt.Println(i)

		if err != nil {
			return buffer, 0, err
		}
	}

	columns := make([]struct {
		words     int // number of words (confirmed to fit in last loop)
		wordsNext int // number of words (tested in current loop iteration)
		values    int // number of values contained
	}, len(values))

	valuesTotal := 1
	wordsTotal := 0

	// maximum number of words in a block (header uses 3 words)
	const wordsMax = 4096/8 - 3
	valuesMax := len(values[0])

	// go over possible numbers of values to store in block
	// and keep track of number of words required for each column
	for {
		wordsTotalNext := wordsTotal
		// check if column needs one more word to store valuesTotal
		for i := range columns {
			if columns[i].values < valuesTotal {
				count, err := simple8b.Count(encoded[i][columns[i].words])

				if err != nil {
					return buffer, 0, err
				}

				wordsTotalNext++
				columns[i].wordsNext++
				columns[i].values += count
			}
		}

		// valuesTotal exceeds block capacity, do not update columns.words lower valuesTotal again
		if wordsTotalNext > wordsMax {
			valuesTotal--
			break
		}

		// valuesTotal fits inside block, update columns.words and wordsTotal
		wordsTotal = wordsTotalNext
		for i := range columns {
			columns[i].words = columns[i].wordsNext
		}

		if valuesTotal == valuesMax {
			break
		}

		valuesTotal++
	}

	bytesTotal := 8 * (wordsTotal + 3)

	header := BlockHeader{
		BlockVersion: 1,
		NumPoints:    uint32(valuesTotal),
		NumColumns:   uint8(len(values)),
		TimeFirst:    values[0][0],
		TimeLast:     values[0][valuesTotal-1],
		BytesUsed:    uint16(bytesTotal),
	}

	buffer.Grow(4096)

	// write header
	if err := binary.Write(&buffer, binary.LittleEndian, header); err != nil {
		return buffer, 0, err
	}

	// write columns
	for i := range columns {
		if err := binary.Write(&buffer, binary.LittleEndian, encoded[i][:columns[i].words]); err != nil {
			return buffer, 0, err
		}
	}

	// check if calculated length matches buffer length (should never fail)
	if buffer.Len() != bytesTotal {
		return buffer, 0, errors.New("buffer length does not match calculation")
	}

	// make sure buffer length is 4096 bytes
	if err := binary.Write(&buffer, binary.LittleEndian, make([]uint8, 4096-bytesTotal)); err != nil {
		return buffer, 0, err
	}

	return buffer, valuesTotal, nil
}
