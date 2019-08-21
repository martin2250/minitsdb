package downsampling

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerMean downsamplerMean

type downsamplerMean struct{}

func (downsamplerMean) DownsampleFirst(i []int64) int64 {
	var sum int64
	for _, c := range i {
		sum += c
	}
	return sum / int64(len(i))
}

//func (downsamplerMean) Needs() DownSamplingType {
//	return Sum
//}
//
//func (downsamplerMean) Downsample(i DownsampleInput) int64 {
//	var sum int64
//	for _, c := range i.Sum {
//		sum += c
//	}
//	return sum
//}
