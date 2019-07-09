package series

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"time"
)

// Bucket is a downsampling step
// Bucket only describes data stored permanently in files, not
type Bucket struct {
	series        Series
	TimeLast      int64 // timestamp of last value stored in file (indicates when to downsample data)
	TimeStep      int64 // time between points
	PointsPerFile int64
	Path          string
}

// Query is used to read data from a bucket
type Query struct {
	bucket          Bucket
	currentFileTime int64
	currentFile     *os.File
	TimeFrom        int64
	TimeTo          int64
}

// ErrQueryEnd indicated no more values to read
var ErrQueryEnd = fmt.Errorf("Query has no more values")

// ReadNextBlock reads one 4k block and returns the values as values[point][column]
// when there are no more points to read, ErrQueryEnd returned. values might still contain valid data
// subsequent reads will also return ErrQueryEnd
func (q Query) ReadNextBlock() (values [][]int64, err error) {
	return
}

// CreateQuery creates a Query on a Bucket
// from, to: time range
// columns: list of columns to return
func (bucket Bucket) CreateQuery(from, to int64, columns []int) (q Query, err error) {
	q.TimeFrom = from % bucket.TimeStep
	q.TimeTo = to % bucket.TimeStep

	q.bucket = bucket
	q.currentFileTime = q.TimeFrom

	files, err := ioutil.ReadDir(bucket.Path)

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var fileStartTime int64
		if n, err := fmt.Sscanf(file.Name(), "%d.mdb", &fileStartTime); err == nil && n == 1 {
			q.currentFile, err = os.Open(file.Name())

			return q, nil
		}
	}

	err = ErrQueryEnd
	return
}

// Series describes a time series, id'd by a name and tags
type Series struct {
	Values        [][]int64
	OverwriteLast bool // data buffer contains last block on disk, overwrite
	Options       Options
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
