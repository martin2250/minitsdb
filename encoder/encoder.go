package encoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/jwilder/encoding/simple8b"
)

// BlockHeader specifies the binary structure of
// the header stored at the beginning of each block
type BlockHeader struct {
	// specifies the Encoder used to encode the Block
	BlockVersion uint8
	// number of columns stored in block
	NumColumns uint8
	// number of points stored in block
	NumPoints uint32
	// number of used bytes in 4k block
	BytesUsed uint16
	// timestamp of the first data point
	TimeFirst int64
	// timestamp of the last data point
	TimeLast int64
}

// DecodeHeader reads the block header from a 4096-byte data block
func DecodeHeader(r io.Reader) (BlockHeader, error) {
	var header BlockHeader

	err := binary.Read(r, binary.LittleEndian, &header)

	return header, err
}

// DecodeBlock decodes values from a 4k block
func DecodeBlock(r io.Reader) (BlockHeader, [][]int64, error) {
	header, err := DecodeHeader(r)
	if err != nil {
		return header, nil, err
	}

	values := make([][]int64, header.NumColumns)

	for i := range values {
		transformed := make([]uint64, header.NumPoints)
		points := 0

		for points < int(header.NumPoints) {
			var buf [240]uint64
			var encoded uint64

			if err := binary.Read(r, binary.LittleEndian, &encoded); err != nil {
				return header, nil, err
			}

			n, err := simple8b.Decode(&buf, encoded)

			if err != nil {
				return header, nil, err
			}

			copy(transformed[points:], buf[:n])
			points += n
		}

		values[i] = undoTransform(transformed)
	}

	return header, values, nil
}

// EncodeBlock encodes as many values into a 4k block as it can possibly fit
// returns the binary block and the number of data points contained within
func EncodeBlock(values [][]int64) (bytes.Buffer, int, error) {
	encoded := make([][]uint64, len(values))
	var buffer bytes.Buffer

	for i := range values {
		var err error

		encoded[i], err = simple8b.EncodeAll(doTransform(values[i]))

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
