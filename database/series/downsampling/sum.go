package downsampling

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerSum downsamplerSum

type downsamplerSum struct{}

func (downsamplerSum) DownsampleFirst(i []int64) int64 {
	var sum int64
	for _, c := range i {
		sum += c
	}
	return sum
}

//func (downsamplerSum) Needs() DownSamplingType {
//	return Sum
//}
//
//func (downsamplerSum) Downsample(i DownsampleInput) int64 {
//	var sum int64
//	for _, c := range i.Sum {
//		sum += c
//	}
//	return sum
//}
