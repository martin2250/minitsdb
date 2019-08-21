package downsampling

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerMin downsamplerMin

type downsamplerMin struct{}

func (downsamplerMin) DownsampleFirst(i []int64) int64 {
	min := i[0]
	for _, c := range i[1:] {
		if c < min {
			min = c
		}
	}
	return min
}
