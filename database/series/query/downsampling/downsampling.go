package downsampling

//type DownSamplingType int
//
//const (
//	Sum DownSamplingType = 1 << iota
//	SumOfSquares
//	Min
//	Max
//	First
//	Last
//	StdDev
//)

//// todo: rename this
//// DownsampleInput holds
//type DownsampleInput struct {
//	Count        []int64
//	Sum          []int64
//	SumOfSquares []int64
//	Min          []int64
//	Max          []int64
//	First        []int64
//	Last         []int64
//}

type Downsampler interface {
	DownsampleFirst(i []int64) int64
	//Downsample(i DownsampleInput) int64
	//Needs() DownSamplingType
}
