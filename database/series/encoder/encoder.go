package encoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/jwilder/encoding/simple8b"
	"github.com/martin2250/minitsdb/util"
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

type decoderState int

const (
	stateHeader decoderState = iota
	stateBody
	stateError
)

type Decoder struct {
	// Header contains the last BlockHeader read
	Header BlockHeader
	r      io.Reader
	s      decoderState
	// Columns contains a list of columns that should be decoded
	// must be sorted by Index
	Columns []struct {
		Index       int
		Transformer Transformer
	}
}

func NewDecoder(r io.Reader) Decoder {
	return Decoder{
		r: r,
		s: stateHeader,
	}
}

// DecodeHeader reads the next block header
func (d *Decoder) DecodeHeader() (BlockHeader, error) {
	switch d.s {
	case stateError:
		return BlockHeader{}, errors.New("decoder is in error state")
	case stateBody:
		_, err := io.CopyN(ioutil.Discard, d.r, 4096-8*3)
		if err != nil {
			d.s = stateError
			return BlockHeader{}, err
		}
	}
	err := binary.Read(d.r, binary.LittleEndian, &d.Header)

	if err != nil {
		d.s = stateError
		return BlockHeader{}, err
	}

	d.s = stateBody
	return d.Header, err
}

// DecodeBlock decodes the next block from the reader
func (d *Decoder) DecodeBlock() ([][]int64, error) {
	switch d.s {
	case stateError:
		return nil, errors.New("decoder is in error state")
	case stateHeader:
		_, err := d.DecodeHeader()
		if err != nil {
			d.s = stateError
			return nil, err
		}
	}

	values := make([][]int64, len(d.Columns))
	var (
		// number of words read from r
		wordsRead int = 3
		// number of columns read from the block
		colsRead int
	)

	for i, col := range d.Columns {
		// check if file even contains this column
		if col.Index >= int(d.Header.NumColumns) {
			d.s = stateError
			return nil, errors.New("not enough columns in block")
		}
		// skip columns that are not required
		for colsRead < col.Index {
			var pointsRead int
			for pointsRead < int(d.Header.NumPoints) {
				if wordsRead == 512 {
					d.s = stateError
					return nil, errors.New("column not complete at end of block")
				}
				var encoded uint64
				err := binary.Read(d.r, binary.LittleEndian, &encoded)
				if err != nil {
					d.s = stateError
					return nil, err
				}
				wordsRead++
				c, err := simple8b.Count(encoded)
				if err != nil {
					d.s = stateError
					return nil, err
				}
				pointsRead += c
			}
			colsRead++
		}

		transformed := make([]uint64, d.Header.NumPoints)

		var pointsRead int
		for pointsRead < int(d.Header.NumPoints) {
			if wordsRead == 512 {
				d.s = stateError
				return nil, errors.New("column not complete at end of block")
			}

			var buf [240]uint64
			var encoded uint64
			if err := binary.Read(d.r, binary.LittleEndian, &encoded); err != nil {
				d.s = stateError
				return nil, err
			}
			wordsRead++
			c, err := simple8b.Decode(&buf, encoded)
			if err != nil {
				d.s = stateError
				return nil, err
			}
			copy(transformed[pointsRead:], buf[:c])
			pointsRead += c
		}

		var err error
		values[i], err = col.Transformer.Revert(transformed)
		if err != nil {
			d.s = stateError
			return nil, err
		}
	}

	// discard rest of block
	_, err := io.CopyN(ioutil.Discard, d.r, int64(8*(512-wordsRead)))
	if err != nil {
		d.s = stateError
		return nil, err
	}

	d.s = stateHeader
	return values, nil
}

// ReadBlock reads exactly 4096 bytes from a io.Reader and decodes the block contained
func ReadBlock(r io.Reader) (BlockHeader, [][]int64, error) {
	b, err := util.ReadBlock(r)

	if err != nil {
		return BlockHeader{}, nil, err
	}

	return DecodeBlock(&b, nil)
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
