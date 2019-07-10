package series

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"time"
)

// Series describes a time series, id'd by a name and tags
type Series struct {
	Values        [][]int64
	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Options       Options
	Path          string
}

// Column holds the json structure that describes a column in a series
type Column struct {
	Name     string
	Decimals int
}

// DownsampleStep ...
type DownsampleStep struct {
}

// Options is an interface to the json file that describes the series
type Options struct {
	Name              string
	Path              string
	Tags              map[string]string
	DownsamplingSteps []DownsampleStep
	FlushInterval     time.Duration // force write to disk after x seconds
	Capacity          int           // will write to disk when half full
	ReuseBytes        int           // reuse last block in file if less bytes are full
	Columns           map[string]Column
}

func readOptions(seriespath string) (Options, error) {
	options := Options{}

	file, err := os.Open(path.Join(seriespath, "options.json"))
	if err != nil {
		return options, err
	}
	defer file.Close()

	content, err := ioutil.ReadAll(file)
	if err != nil {
		return options, err
	}

	err = json.Unmarshal(content, &options)

	return options, err
}

// OpenSeries opens series from file
func OpenSeries(seriespath string) (Series, error) {
	series := Series{}
	var err error
	series.Options, err = readOptions(seriespath)

	if err != nil {
		return series, err
	}

	return series, nil
}
