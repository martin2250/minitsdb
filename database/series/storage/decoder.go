package storage

import (
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"

	"github.com/jwilder/encoding/simple8b"
)

type decoderState int

const (
	stateError decoderState = iota
	stateHeader
	stateBody
)

type Decoder struct {
	// Header contains the last BlockHeader read
	Header BlockHeader
	reader io.Reader
	s      decoderState
	// Columns contains a sorted list of columns that should be read
	Columns []int
}

func NewDecoder() Decoder {
	return Decoder{
		s: stateError,
	}
}

func (d *Decoder) skipWords(words int) error {
	var err error
	switch r := d.reader.(type) {
	case io.Seeker:
		_, err = r.Seek(int64(8*words), io.SeekCurrent)
	default:
		_, err = io.CopyN(ioutil.Discard, r, int64(8*words))
	}
	return err
}

// SetReader changes the reader and re-initializes the decoder
func (d *Decoder) SetReader(r io.Reader) {
	d.reader = r
	d.s = stateHeader
}

// DecodeHeader reads the next block header
func (d *Decoder) DecodeHeader() (BlockHeader, error) {
	// check state
	switch d.s {
	case stateError:
		return BlockHeader{}, errors.New("decoder is in error state")
	case stateBody:
		err := d.skipWords(512 - 3)
		if err != nil {
			d.s = stateError
			return BlockHeader{}, err
		}
	}
	var header blockHeaderRaw
	// read header
	err := binary.Read(d.reader, binary.LittleEndian, &header)

	if err != nil {
		d.s = stateError
		return BlockHeader{}, err
	}

	d.Header = header.Nice()
	d.s = stateBody
	return d.Header, err
}

// DecodeBlock decodes the next block from the reader
func (d *Decoder) DecodeBlock() ([][]uint64, error) {
	// check state
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

	values := make([][]uint64, len(d.Columns))

	// number of words left in block
	wordsLeft := 512 - 3
	// number of columns read from the block
	colsRead := 0

	for i, col := range d.Columns {
		// check if file even contains this column
		if col >= d.Header.NumColumns {
			d.s = stateError
			return nil, errors.New("not enough columns in block")
		}
		if col < colsRead {
			return nil, errors.New("decoder columns are not in order")
		}
		// skip columns that are not required
		for colsRead < col {
			// number of points read from this column
			var pointsRead int
			for pointsRead < d.Header.NumPoints {
				// check if there are words left in this block
				if wordsLeft == 0 {
					d.s = stateError
					return nil, errors.New("column not complete at end of block")
				}
				// read word
				var encoded uint64
				if err := binary.Read(d.reader, binary.LittleEndian, &encoded); err != nil {
					d.s = stateError
					return nil, err
				}
				wordsLeft--
				// check how many values this word contains
				c, err := simple8b.Count(encoded)
				if err != nil {
					d.s = stateError
					return nil, err
				}
				pointsRead += c
			}
			colsRead++
		}

		values[i] = make([]uint64, d.Header.NumPoints)

		// read points into output array
		var pointsRead int
		for pointsRead < int(d.Header.NumPoints) {
			// check if there are words left in this block
			if wordsLeft == 0 {
				d.s = stateError
				return nil, errors.New("column not complete at end of block")
			}
			// read word
			var encoded uint64
			if err := binary.Read(d.reader, binary.LittleEndian, &encoded); err != nil {
				d.s = stateError
				return nil, err
			}
			wordsLeft--
			// decode word
			var buf [240]uint64
			c, err := simple8b.Decode(&buf, encoded)
			if err != nil {
				d.s = stateError
				return nil, err
			}
			// copy decoded raw values to buffer
			copy(values[i][pointsRead:], buf[:c])
			// add c to pointsRead after copy
			pointsRead += c
		}
		colsRead++
	}

	// discard rest of block so reader is at next header
	err := d.skipWords(wordsLeft)
	if err != nil {
		d.s = stateError
		return nil, err
	}

	d.s = stateHeader
	return values, nil
}
