package encoder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/martin2250/minitsdb/encoder/encoders/simple8bv1"
)

// BlockHeader specifies the binary structure of
// the header stored at the beginning of each block
type BlockHeader struct {
	// specifies the Encoder used to encode the Block
	BlockType uint16
	// timestamp of the first data point
	TimeFirst uint64
	// timestamp of the last data point
	TimeLast uint64
	// number of points stored in block
	NumPoints uint16
	// number of variables stored in block
	NumValues uint16
}

// DecodeHeader reads the block header from a 4096-byte data block
func DecodeHeader(block []byte) (BlockHeader, error) {
	var header BlockHeader

	r := bytes.NewReader(block)

	err := binary.Read(r, binary.LittleEndian, &header)

	return header, err
}

// DecodeBlock decodes values from a 4k block
func DecodeBlock() {

}

// EncodeBlock encodes as many values into a 4k block as it can possibly fit
func EncodeBlock(data [][]uint64) ([]byte, int, error) {

}

func getCodingMethods(id uint16) (encodeMethods, error) {
	switch id {
	case math.MaxUint16:
	case 1:
		return encodeMethods{
			simple8bv1.EncodeSimple8bv1,
			simple8bv1.DecodeSimple8bv1,
		}, nil
	}
	return encodeMethods{}, errors.New("unknown block type")
}

type encodeMethods struct {
	EncodeMethod func(data [][]uint64) ([]byte, int, error)
	DecodeMethod func(block []byte) ([][]uint64, error)
}
