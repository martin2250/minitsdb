package downsampling

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerMax downsamplerMax

type downsamplerMax struct{}

func (downsamplerMax) DownsampleFirst(i []int64) int64 {
	max := i[0]
	for _, c := range i[1:] {
		if c > max {
			max = c
		}
	}
	return max
}
