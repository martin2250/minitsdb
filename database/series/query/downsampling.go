package query

type DownSamplingType int

const (
	Sum DownSamplingType = 1 << iota
	SumOfSquares
	Min
	Max
	First
	Last
	StdDev
)

// todo: rename this
// DownsampleInput holds
type DownsampleInput struct {
	Count        []int64
	Sum          []int64
	SumOfSquares []int64
	Min          []int64
	Max          []int64
	First        []int64
	Last         []int64
}

type Downsampler interface {
	DownsampleFirst(i []int64) int64
	Downsample(i DownsampleInput) int64
	Needs() DownSamplingType
}

// DownsamplerCount calculates the number of values in the input
var DownsamplerCount downsamplerCount

type downsamplerCount struct{}

func (downsamplerCount) Needs() DownSamplingType {
	return 0
}

func (downsamplerCount) Downsample(i DownsampleInput) int64 {
	var count int64
	for _, c := range i.Count {
		count += c
	}
	return count
}

func (downsamplerCount) DownsampleFirst(i []int64) int64 {
	return int64(len(i))
}

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerSum downsamplerSum

type downsamplerSum struct{}

func (downsamplerSum) Needs() DownSamplingType {
	return Sum
}

func (downsamplerSum) Downsample(i DownsampleInput) int64 {
	var sum int64
	for _, c := range i.Sum {
		sum += c
	}
	return sum
}

func (downsamplerSum) DownsampleFirst(i []int64) int64 {
	var sum int64
	for _, c := range i {
		sum += c
	}
	return sum
}

// DownsamplerSum calculates the sum of all values in the input
var DownsamplerMean downsamplerMean

type downsamplerMean struct{}

func (downsamplerMean) Needs() DownSamplingType {
	return Sum
}

func (downsamplerMean) Downsample(i DownsampleInput) int64 {
	var sum int64
	for _, c := range i.Sum {
		sum += c
	}
	return sum
}

func (downsamplerMean) DownsampleFirst(i []int64) int64 {
	var sum int64
	for _, c := range i {
		sum += c
	}
	return sum / int64(len(i))
}
