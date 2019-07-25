package series

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"

	"github.com/martin2250/minitsdb/util"

	"github.com/golang/glog"
	"github.com/martin2250/minitsdb/encoder"
)

// DataFile represents a file in the bucket's database directory
type DataFile struct {
	TimeStart int64
	Blocks    int64
	Path      string
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
	PointsPerFile  int64

	DataFiles []DataFile

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

		if fileStartTime%(b.TimeResolution*b.PointsPerFile) != 0 {
			glog.Warningf("stray file %s", file.Name())
			continue
		}

		list = append(list, fileStartTime)
	}

	return list, nil
}

func (b *Bucket) loadFiles() error {
	files, err := ioutil.ReadDir(b.GetPath())

	if err != nil {
		return err
	}

	b.DataFiles = make([]DataFile, 0)

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

		if fileStartTime%(b.TimeResolution*b.PointsPerFile) != 0 {
			glog.Warningf("stray file %s", file.Name())
			continue
		}

		size := file.Size()

		if size%4096 != 0 {
			return errors.New("file damaged")
		}

		blocks := size / 4096

		// don't include empty files
		if blocks < 1 {
			glog.Warningf("empty file %s", file.Name())
			continue
		}

		df := DataFile{
			Path:      path.Join(b.GetPath(), file.Name()),
			TimeStart: fileStartTime,
			Blocks:    blocks,
		}

		b.DataFiles = append(b.DataFiles, df)
	}

	return nil
}

// getLastBlock returns a buffer with the last block of the last file
// returns io.EOF if there is no block to return
func (b Bucket) getLastBlock() (bytes.Buffer, error) {
	if len(b.DataFiles) < 1 {
		return bytes.Buffer{}, io.EOF
	}

	df := b.DataFiles[len(b.DataFiles)-1]

	// open file
	file, err := os.Open(df.Path)

	if err != nil {
		return bytes.Buffer{}, err
	}

	defer file.Close()

	// seek last block
	if _, err = file.Seek((df.Blocks-1)*4096, io.SeekStart); err != nil {
		return bytes.Buffer{}, err
	}

	return util.ReadBlock(file)
}

// checkTimeLast sets TimeLast from last block on disk
// this function is called by LoadSeries
func (b *Bucket) checkTimeLast() error {
	times, err := b.GetDataFiles()

	if err != nil {
		return err
	}

	// no file to read
	if len(times) < 1 {
		glog.Info("no files in bucket")
		return nil
	}

	lastPath := b.GetFileName(times[len(times)-1])

	// check file size
	stat, err := os.Stat(lastPath)

	if err != nil {
		return err
	}

	size := stat.Size()

	if size%4096 != 0 {
		return errors.New("file damaged")
	}

	blocks := size / 4096

	// no blocks to read
	if blocks < 1 {
		return nil
	}

	// open file
	file, err := os.Open(lastPath)

	if err != nil {
		return err
	}

	defer file.Close()

	// seek last block
	if _, err = file.Seek((blocks-1)*4096, io.SeekStart); err != nil {
		return err
	}

	// read last block
	header, err := encoder.DecodeHeader(file)

	if err != nil {
		return err
	}

	b.TimeLast = header.TimeLast

	return nil
}
