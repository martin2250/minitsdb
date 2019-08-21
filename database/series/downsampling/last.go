package downsampling

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerLast downsamplerLast

type downsamplerLast struct{}

func (downsamplerLast) DownsampleFirst(i []int64) int64 {
	return i[len(i)-1]
}
