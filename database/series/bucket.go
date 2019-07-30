package series

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"strconv"

	"github.com/martin2250/minitsdb/util"

	"github.com/golang/glog"
	"github.com/martin2250/minitsdb/database/series/storage"
)

// DataFile represents a file in the bucket's database directory
type DataFile struct {
	Blocks int64
	Path   string
}

// Bucket is a downsampling step
// Bucket only describes data stored permanently in files, not the data buffered in RAM
type Bucket struct {
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

	d := storage.NewDecoder()
	d.SetReader(&buf)

	// read last block header
	header, err := d.DecodeHeader()

	if err != nil {
		return err
	}

	b.TimeLast = header.TimeLast

	return nil
}

// Init loads relevant data from disk
func (b *Bucket) Init() error {
	if err := b.loadFiles(); err != nil {
		return err
	}

	if err := b.checkTimeLast(); err != nil {
		return err
	}
	// todo: check that total number of columns is smaller or equal to 256

	return nil
}

// GetStorageTime checks how many points fit into the same file as the first point
// returns the number of points that fit and the time at which the file starts
func (b Bucket) GetStorageTime(time []int64) (int, int64) {
	count := len(time)
	timeFileStart := util.RoundDown(time[0], int64(b.PointsPerFile)*b.TimeResolution)

	for i, t := range time {
		if timeFileStart != util.RoundDown(t, int64(b.PointsPerFile)*b.TimeResolution) {
			count = i
			break
		}
	}

	return count, timeFileStart
}

func (b *Bucket) WriteBlock(fileTime int64, buffer []byte, overwrite bool) error {
	if len(buffer)%4096 != 0 {
		return errors.New("buffer length not divisible by 4096")
	}
	if fileTime%(int64(b.PointsPerFile)*b.TimeResolution) != 0 {
		return errors.New("file time invalid")
	}

	// create new data file if not indexed yet
	_, exists := b.DataFiles[fileTime]

	if !exists {
		b.DataFiles[fileTime] = &DataFile{
			Blocks: 0,
			Path:   b.GetFileName(fileTime),
		}
	}

	stat, err := os.Stat(b.DataFiles[fileTime].Path)

	// if this file name exists, check the file before continuing
	if err == nil {
		if stat.IsDir() {
			// todo: log
			fmt.Printf("ERROR: data file is directory: %s", b.DataFiles[fileTime].Path)
			return errors.New("data file is directory")
		}
		if stat.Size()%4096 != 0 {
			fmt.Printf("ERROR: data file size %d not multiple of 4096", stat.Size())
			return fmt.Errorf("data file size %d not multiple of 4096", stat.Size())
		}
	} else if !os.IsNotExist(err) {
		fmt.Printf("ERROR: unknown error while statting %s", err.Error())
		return err
	}

	// open file for writing
	file, err := os.OpenFile(b.DataFiles[fileTime].Path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		fmt.Printf("error while opening %s for writing", err.Error())
		return err
	}

	defer file.Close()

	// seek last block if it should be overwritten
	if overwrite && stat.Size() > 0 {
		_, err := file.Seek(-4096, io.SeekEnd)

		if err != nil {
			fmt.Printf("error while seeking start of last block of %s: %s", b.DataFiles[fileTime].Path, err.Error())
			return err
		}
	}

	// write block to file
	n, err := file.Write(buffer)

	if err != nil {
		if n%4096 != 0 {
			fmt.Printf("ERROR: wrote incomplete block to %s (%d bytes) because of %s", b.DataFiles[fileTime].Path, n, err.Error())
		}
		return err
	}

	b.DataFiles[fileTime].Blocks++
	return nil
}
