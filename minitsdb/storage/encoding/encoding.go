package encoding

// BlockHeader is a more usable version of the header stored at the beginning of each block
type BlockHeader struct {
	// specifies the Encoder used to encode the Block
	BlockVersion int
	// number of columns stored in block
	NumColumns int
	// number of points stored in block
	NumPoints int
	// number of used bytes in 4k block
	BytesUsed int
	// timestamp of the first data point
	TimeFirst int64
	// timestamp of the last data point
	TimeLast int64
}

// blockHeaderRaw specifies the binary structure of
// the header stored at the beginning of each block
type blockHeaderRaw struct {
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

func (b blockHeaderRaw) Nice() BlockHeader {
	return BlockHeader{
		BlockVersion: int(b.BlockVersion),
		NumColumns:   int(b.NumColumns),
		NumPoints:    int(b.NumPoints),
		BytesUsed:    int(b.BytesUsed),
		TimeFirst:    b.TimeFirst,
		TimeLast:     b.TimeLast,
	}
}
