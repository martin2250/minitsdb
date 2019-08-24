package storage

import (
	"bytes"
	"fmt"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	"github.com/martin2250/minitsdb/util"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
)

// Bucket is a downsampling step
// Bucket only describes data stored permanently in files, not the data buffered in RAM
type Bucket struct {
	// timestamp of last value stored in file (indicates when to downsample data)
	TimeLast int64
	// time between points
	TimeResolution int64

	// number of points in a file
	PointsPerFile int64

	// index: start time
	DataFiles []*DataFile

	Path string
}

func (b *Bucket) sortFiles() {
	sort.Slice(b.DataFiles, func(i, j int) bool {
		return b.DataFiles[i].TimeStart < b.DataFiles[j].TimeStart
	})
}

func (b *Bucket) loadFiles() error {
	b.DataFiles = make([]*DataFile, 0, 16)

	// list database files
	fileInfos, err := ioutil.ReadDir(b.Path)

	// create if not exists
	if os.IsNotExist(err) {
		err = os.Mkdir(b.Path, 0755)
		return nil
	} else if err != nil {
		return err
	}

	for _, info := range fileInfos {
		file, err := OpenDataFile(path.Join(b.Path, info.Name()), info, b.TimeResolution*b.PointsPerFile)

		if err != nil {
			// errors are non-fatal
			fmt.Println(err.Error())
			continue
		}

		b.DataFiles = append(b.DataFiles, &file)
	}

	// should already be sorted as ioutil returns list of files sorted
	b.sortFiles()

	return nil
}

// checkTimeLast sets TimeLast from last block on disk
func (b *Bucket) checkTimeLast() error {
	b.TimeLast = math.MinInt64

	if len(b.DataFiles) == 0 {
		return nil
	}

	// read last block
	df := b.DataFiles[len(b.DataFiles)-1]
	buf, err := df.ReadBlock(df.Blocks - 1)

	if err != nil {
		return err
	}

	d := encoding.NewDecoder()
	d.SetReader(&buf)

	// read last block header
	header, err := d.DecodeHeader()

	if err != nil {
		return err
	}

	b.TimeLast = header.TimeLast

	return nil
}

func OpenBucket(basePath string, timeStep int64, pointsPerFile int64) (Bucket, error) {
	b := Bucket{
		TimeLast:       math.MinInt64,
		TimeResolution: timeStep,
		PointsPerFile:  pointsPerFile,
		Path:           path.Join(basePath, strconv.FormatInt(timeStep, 10)),
	}

	err := b.loadFiles()

	if err != nil {
		return Bucket{}, err
	}

	err = b.checkTimeLast()

	if err != nil {
		return Bucket{}, err
	}

	return b, nil
}

func (b *Bucket) createDataFile(fileTime int64) *DataFile {
	fileTime = util.RoundDown(fileTime, b.TimeResolution*b.PointsPerFile)

	for j := range b.DataFiles {
		if b.DataFiles[j].TimeStart == fileTime {
			return b.DataFiles[j]
		}
	}
	b.DataFiles = append(b.DataFiles, NewDataFile(b.Path, fileTime, b.TimeResolution*b.PointsPerFile))

	b.sortFiles()

	return b.DataFiles[len(b.DataFiles)-1]
}

// GetStorageTime checks how many points fit into the same file as the first point
// returns the number of points that fit and the time at which the file starts
func (b Bucket) GetStorageTime(time []int64) (*DataFile, int) {
	// find data file
	dataFile := b.createDataFile(time[0])

	// find all points that fit into this file
	for i, t := range time {
		if t > dataFile.TimeEnd {
			return dataFile, i
		}
	}

	return dataFile, len(time)
}

func (b *Bucket) WriteBlock(fileTime int64, buffer bytes.Buffer, overwrite bool) error {
	dataFile := b.createDataFile(fileTime)

	return dataFile.WriteBlock(buffer, overwrite)
}
