package downsampling

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerFirst downsamplerFirst

type downsamplerFirst struct{}

func (downsamplerFirst) DownsampleFirst(i []int64) int64 {
	return i[0]
}
