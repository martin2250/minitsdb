package storage

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/martin2250/minitsdb/database/series/storage/encoding"
	"github.com/martin2250/minitsdb/util"
	"io"
	"log"
	"os"
	"path"
	"sync"
)

// DataFile represents a file in the bucket's database directory
type DataFile struct {
	// Path to file (absolute)
	Path string
	// Number of blocks in file (fill this value using the Update function)
	Blocks int64
	// TimeStart and TimeEnd hold the time range stored in this file
	TimeStart int64
	TimeEnd   int64
	// don't read while writing (only lock while actually reading / writing a block, file may be opened by multiple workers at once)
	Mut sync.RWMutex
}

// ReadBlock reads the n-th block of a file
func (f *DataFile) ReadBlock(n int64) (bytes.Buffer, error) {
	file, err := os.Open(f.Path)

	if err != nil {
		return bytes.Buffer{}, err
	}

	defer file.Close()

	if _, err = file.Seek(n*encoding.BlockSize, io.SeekStart); err != nil {
		return bytes.Buffer{}, err
	}

	f.Mut.RLock()
	buf, err := util.ReadBlock(file)
	f.Mut.RUnlock()

	return buf, err
}

// Write a block to the data file,
func (f *DataFile) WriteBlock(buffer bytes.Buffer, overwrite bool) error {
	var file *os.File
	var err error

	if buffer.Len() != encoding.BlockSize {
		return errors.New("buffer length does not equal block size")
	}

	// can't overwrite when file has no blocks
	if f.Blocks == 0 {
		overwrite = false
	}

	f.Mut.Lock()
	defer f.Mut.Unlock()

	// only use seek when overwriting
	if overwrite {
		file, err = os.OpenFile(f.Path, os.O_CREATE|os.O_WRONLY, 0644)
	} else {
		file, err = os.OpenFile(f.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	}

	if err != nil {
		return err
	}

	defer file.Close()

	if overwrite {
		_, err = file.Seek((f.Blocks-1)*encoding.BlockSize, io.SeekStart)
		if err != nil {
			return err
		}
	}

	n, err := file.Write(buffer.Bytes())

	if err != nil {
		if n%4096 != 0 {
			log.Panicf("ERROR: wrote incomplete block to %s (%d bytes) because of %s", f.Path, n, err.Error())
		}
		return err
	}

	if !overwrite {
		f.Blocks++
	}

	return nil
}

// DataFileReader is a Reader that automatically locks the datafile's mutex for every call to read
type DataFileReader struct {
	mut *sync.RWMutex
	f   *os.File
}

func (r *DataFileReader) Read(p []byte) (n int, err error) {
	r.mut.RLock()
	n, err = r.f.Read(p)
	r.mut.RUnlock()
	return
}

func (r *DataFileReader) Seek(offset int64, whence int) (ret int64, err error) {
	return r.f.Seek(offset, whence)
}

func (r *DataFileReader) Close() error {
	return r.f.Close()
}

func (f *DataFile) GetReader() (*DataFileReader, error) {
	handle, err := os.Open(f.Path)

	if err != nil {
		return nil, err
	}

	r := DataFileReader{
		mut: &f.Mut,
		f:   handle,
	}

	return &r, nil
}

func NewDataFile(basePath string, timeStart int64, timeRange int64) *DataFile {
	return &DataFile{
		Path:      path.Join(basePath, fmt.Sprintf("%011d.mdb", timeStart)),
		Blocks:    0,
		TimeStart: timeStart,
		TimeEnd:   timeStart + timeRange - 1,
	}
}

// OpenDataFile loads an existing data file
// errors are non fatal but indicate that the file is not a datafile
func OpenDataFile(filePath string, info os.FileInfo, timeRange int64) (DataFile, error) {
	df := DataFile{
		Path:   filePath,
		Blocks: 0,
	}

	// check if info is file
	if info.IsDir() {
		return DataFile{}, fmt.Errorf("data file %s is actually a directory", filePath)
	}

	// check if file name matches format and read start time
	if n, err := fmt.Sscanf(info.Name(), "%d.mdb", &df.TimeStart); err != nil || n != 1 {
		return DataFile{}, fmt.Errorf("file %s is not a data file", filePath)
	}

	// check if start time is a multiple of timeRange
	if df.TimeStart%timeRange != 0 {
		return DataFile{}, fmt.Errorf("file %s does not fit expected time range", filePath)
	}

	df.TimeEnd = df.TimeStart + timeRange - 1

	// read size of file and calculate number of blocks
	size := info.Size()

	if size%encoding.BlockSize != 0 {
		return DataFile{}, fmt.Errorf("size %d of %s is not a multiple of block size", size, filePath)
	}

	df.Blocks = size / encoding.BlockSize

	if df.Blocks == 0 {
		return DataFile{}, fmt.Errorf("file %s is empty", filePath)
	}

	return df, nil
}
