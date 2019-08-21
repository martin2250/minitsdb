package downsampling

// DownsamplerCount calculates the number of values in the input
var DownsamplerCount downsamplerCount

type downsamplerCount struct{}

func (downsamplerCount) DownsampleFirst(i []int64) int64 {
	return int64(len(i))
}

//func (downsamplerCount) Needs() DownSamplingType {
//	return 0
//}
//
//func (downsamplerCount) Downsample(i DownsampleInput) int64 {
//	var count int64
//	for _, c := range i.Count {
//		count += c
//	}
//	return count
//}
