package storage

import (
	"bytes"
	"fmt"
	"github.com/martin2250/minitsdb/database/series/storage/encoding"
	"github.com/martin2250/minitsdb/util"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
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
	DataFiles []DataFile

	// indicates if this is the first (highest resolution) bucket (contains no aggregations)
	First bool

	Path string
}

func (b *Bucket) loadFiles() error {
	b.DataFiles = make([]DataFile, 0, 16)

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

		b.DataFiles = append(b.DataFiles, file)
	}

	// should already be sorted as ioutil returns list of files sorted
	sort.Slice(b.DataFiles, func(i, j int) bool {
		return b.DataFiles[i].TimeStart < b.DataFiles[j].TimeStart
	})

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

func OpenBucket(path string, timeStep int64, pointsPerFile int64, first bool) (Bucket, error) {
	b := Bucket{
		TimeLast:       math.MinInt64,
		TimeResolution: timeStep,
		PointsPerFile:  pointsPerFile,
		First:          first,
		Path:           path,
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
			return &b.DataFiles[j]
		}
	}
	b.DataFiles = append(b.DataFiles, NewDataFile(b.Path, fileTime, b.TimeResolution*b.PointsPerFile))

	return &b.DataFiles[len(b.DataFiles)-1]
}

func (b *Bucket) WriteBlock(fileTime int64, buffer bytes.Buffer, overwrite bool) error {
	dataFile := b.createDataFile(fileTime)

	return dataFile.WriteBlock(buffer, overwrite)
}

// WriteData tries to write as many points from the buffer into the bucket, returns the header of the block written or an error
func (b *Bucket) WriteData(buffer PointBuffer, transformers []encoding.Transformer, overwrite bool) (encoding.BlockHeader, error) {
	count := buffer.Len()
	if count == 0 {
		return encoding.BlockHeader{}, nil
	}

	// find data file
	dataFile := b.createDataFile(buffer.Time[0])

	// find all points that fit into this file
	for i, t := range buffer.Time {
		if t > dataFile.TimeEnd {
			count = i
			break
		}
	}

	// transform values
	var err error
	transformed := make([][]uint64, len(transformers)+1)

	transformed[0], err = encoding.TimeTransformer.Apply(buffer.Time[:count])
	if err != nil {
		return encoding.BlockHeader{}, err
	}

	for i, t := range transformers {
		transformed[i+1], err = t.Apply(buffer.Values[i][:count])
		if err != nil {
			return encoding.BlockHeader{}, err
		}
	}

	// encode values
	var block bytes.Buffer
	header, err := encoding.EncodeBlock(&block, s.Buffer.Time[:count], transformed)

	if err != nil {
		return encoding.BlockHeader{}, err
	}

	// write transformed values to file
	err = dataFile.WriteBlock(block, overwrite)

	return header, nil
}
