package series

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"

	"github.com/martin2250/minitsdb/util"

	"github.com/golang/glog"
	"github.com/martin2250/minitsdb/database/series/encoder"
)

// DataFile represents a file in the bucket's database directory
type DataFile struct {
	Blocks int64
	Path   string
}

// Bucket is a downsampling step
// Bucket only describes data stored permanently in files, not the data buffered in RAM
type Bucket struct {
	// the series this bucket belongs to
	series *Series
	// timestamp of last value stored in file (indicates when to downsample data)
	TimeLast int64
	// time between points
	TimeResolution int64

	// number of points in a file
	PointsPerFile int

	// index: start time
	DataFiles map[int64]*DataFile

	// indicates if this is the first (highest resolution) bucket (contains no aggregations)
	First bool
}

// GetPath returns the path where database files are stored
func (b Bucket) GetPath() string {
	return path.Join(b.series.Path, strconv.FormatInt(b.TimeResolution, 10))
}

// GetFileName returns the name of the database file that starts at time
func (b Bucket) GetFileName(time int64) string {
	return path.Join(b.GetPath(), fmt.Sprintf("%011d.mdb", time))
}

// GetDataFiles returns a list of the starting times of all database files
func (b Bucket) GetDataFiles() ([]int64, error) {
	files, err := ioutil.ReadDir(b.GetPath())

	if err != nil {
		return nil, err
	}

	list := make([]int64, 0)

	for _, file := range files {
		if file.IsDir() {
			glog.Warningf("stray directory %s", file.Name())
			continue
		}

		var fileStartTime int64
		if n, err := fmt.Sscanf(file.Name(), "%d.mdb", &fileStartTime); err != nil || n != 1 {
			glog.Warningf("stray file %s", file.Name())
			continue
		}

		if fileStartTime%(b.TimeResolution*int64(b.PointsPerFile)) != 0 {
			glog.Warningf("stray file %s", file.Name())
			continue
		}

		list = append(list, fileStartTime)
	}

	return list, nil
}

func (b *Bucket) loadFiles() error {
	b.DataFiles = make(map[int64]*DataFile, 0)

	// check if bucket folder exists
	stat, err := os.Stat(b.GetPath())

	// create if not exists
	if os.IsNotExist(err) {
		os.Mkdir(b.GetPath(), 0755)
		return nil
	}

	if err != nil {
		return err
	}

	// check if directory
	if !stat.IsDir() {
		return fmt.Errorf("bucket dir %s is a file", b.GetPath())
	}

	// list database files
	files, err := ioutil.ReadDir(b.GetPath())

	if err != nil {
		return err
	}

	for _, file := range files {
		if file.IsDir() {
			glog.Warningf("stray directory %s", file.Name())
			continue
		}

		var fileStartTime int64
		if n, err := fmt.Sscanf(file.Name(), "%d.mdb", &fileStartTime); err != nil || n != 1 {
			glog.Warningf("stray file %s", file.Name())
			continue
		}

		if fileStartTime%(b.TimeResolution*int64(b.PointsPerFile)) != 0 {
			glog.Warningf("stray file %s", file.Name())
			continue
		}

		size := file.Size()

		if size%4096 != 0 {
			return fmt.Errorf("file damaged %s", file.Name())
		}

		blocks := size / 4096

		// don't include empty files
		if blocks < 1 {
			glog.Warningf("empty file %s", file.Name())
			continue
		}

		df := DataFile{
			Path:   path.Join(b.GetPath(), file.Name()),
			Blocks: blocks,
		}

		b.DataFiles[fileStartTime] = &df
	}

	return nil
}

// getLastBlock returns a buffer with the last block of the last file
// returns io.EOF if there is no block to return
func (b Bucket) getLastBlock() (bytes.Buffer, error) {
	if len(b.DataFiles) < 1 {
		return bytes.Buffer{}, io.EOF
	}

	// find last data file
	var dfLast *DataFile
	var timeLast int64 = math.MinInt64

	for time, df := range b.DataFiles {
		if time > timeLast {
			time = timeLast
			dfLast = df
		}
	}

	// open file
	file, err := os.Open(dfLast.Path)

	if err != nil {
		return bytes.Buffer{}, err
	}

	defer file.Close()

	// seek last block
	if _, err = file.Seek((dfLast.Blocks-1)*4096, io.SeekStart); err != nil {
		return bytes.Buffer{}, err
	}

	return util.ReadBlock(file)
}

// checkTimeLast sets TimeLast from last block on disk
// this function is called by LoadSeries
func (b *Bucket) checkTimeLast() error {
	b.TimeLast = math.MinInt64

	buf, err := b.getLastBlock()

	if err == io.EOF {
		return nil
	}

	if err != nil {
		return err
	}

	// read last block
	header, err := encoder.DecodeHeader(&buf)

	if err != nil {
		return err
	}

	b.TimeLast = header.TimeLast

	return nil
}

// NewBucket creates a bucket and loads relevant data from disk
// todo: read PointsPerFile from config
func NewBucket(s *Series, res int64) (Bucket, error) {
	b := Bucket{
		series:         s,
		TimeResolution: res,
		PointsPerFile:  3600 * 24,
	}

	if err := b.loadFiles(); err != nil {
		return Bucket{}, err
	}

	if err := b.checkTimeLast(); err != nil {
		return Bucket{}, err
	}

	return b, nil
}

// WriteBlock returns the number of values written
func (b *Bucket) WriteBlock(values [][]int64) (int, error) {
	if len(values[0]) == 0 {
		return 0, nil
	}
	// check that all values belong to same file
	indexEnd := -1
	timeFileStart := util.RoundDown(values[0][0], int64(b.PointsPerFile))

	for i, t := range values[0] {
		if timeFileStart != util.RoundDown(t, int64(b.PointsPerFile)) {
			indexEnd = i
			break
		}
	}

	if indexEnd != -1 {
		for i := range values {
			values[i] = values[i][:indexEnd]
		}
	}

	block, count, err := encoder.EncodeBlock(values)

	if err != nil {
		return 0, err
	}

	// create new data file if not indexed yet
	dataFile, ok := b.DataFiles[timeFileStart]

	if !ok {
		dataFile = &DataFile{
			Blocks: 0,
			Path:   b.GetFileName(timeFileStart),
		}
		b.DataFiles[timeFileStart] = dataFile
	} else {
		// check file for corruption
		info, err := os.Stat(dataFile.Path)

		if err != nil {
			return 0, err
		}

		if info.Size()%4096 != 0 {
			return 0, fmt.Errorf("file size %d not multiple of 4096", info.Size())
		}
	}

	// open or create file
	//var file *os.File
	file, err := os.OpenFile(dataFile.Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	//if ok {
	//} else {
	//	file, err = os.Create(dataFile.Path)
	//}

	if err != nil {
		return 0, err
	}

	n, err := block.WriteTo(file)

	if err != nil {
		return 0, err
	}

	if n != 4096 {
		return 0, fmt.Errorf("failed to write full 4096 bytes, only wrote %d", n)
	}

	file.Close()

	dataFile.Blocks++

	return count, nil
}
