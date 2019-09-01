package minitsdb

import (
	"bytes"
	"fmt"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	"github.com/martin2250/minitsdb/minitsdb/types"
	"github.com/martin2250/minitsdb/util"
	"github.com/sirupsen/logrus"
	"io"
	"io/ioutil"

	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"sync"
	"time"
)

type Bucket struct {
	// timestamp of last value stored in file
	LastTimeOnDisk int64
	// time between points
	TimeStep int64

	// number of points in a file
	PointsPerFile int64

	DataFiles []*storage.DataFile

	Path string

	Next *Bucket

	Mux sync.RWMutex

	First bool // todo: replace calls to this with Prev==nil
	Last  bool // todo: replace calls to this with Next==nil

	DownSampleColumns []QueryColumn // todo: replace this with something column-agnostic

	// transformers include time and (for non-primary buckets) the count transformer
	Transformers []encoding.Transformer

	// keeps a list of time ranges (of the next bucket) that were modified since the last downsampling event
	Dirty map[int64]struct{}

	Buffer storage.PointBuffer

	LastFlush time.Time
}

// assumes # of columns matches
// assumes that this time is not already stored on disk
// both assumptions must be checked by the series that holds the buckets when inserting into the first bucket
func (b *Bucket) Insert(p storage.Point) {
	b.Buffer.InsertPoint(p)

	if !b.Last {
		b.Dirty[util.RoundDown(p.Values[0], b.Next.TimeStep)] = struct{}{}
	}
}

// Downsample updates values buffered in the next bucket when a source value has changed in this buffer
// returns true if any points were actually downsampled
func (b *Bucket) Downsample() bool {
	if b.Last {
		return false
	}

	if len(b.Dirty) == 0 {
		return false
	}

	for timeStart := range b.Dirty {
		p, err := DownsamplePoint(b.Buffer, b.DownSampleColumns, types.TimeRangeFromPoint(timeStart, b.Next.TimeStep), b.First)

		if err != nil {
			panic(err) // todo: just for testing; replace with log or similar, this should never actually happen
		}

		b.Next.Insert(p)

		delete(b.Dirty, timeStart)
	}

	return true
}

func (b *Bucket) DownsampleStartup() error {
	if b.Next == nil {
		return nil
	}

	defer func() {
		logrus.Info("finished startup downsampling")
	}()

	query := b.Query(b.DownSampleColumns, types.TimeRange{
		Start: b.Next.LastTimeOnDisk + b.Next.TimeStep,
		End:   math.MaxInt64 - b.Next.TimeStep*100,
	}, b.Next.TimeStep)

	for {
		buffer, err := query.Next()

		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		b.Next.Buffer.AppendBuffer(buffer)

		// todo: don't hardcode this
		if b.Next.Buffer.Len() > 400 {
			b.Next.Flush(b.LastTimeOnDisk, false)
		}
	}
}

// Flush writes the bucket's buffer content to disk
// timeLimt sets the last time that may be written to disk
// force allows the buffer to flush even if it would not fill an entire 4k block
// returns true if points were actually written to disk
func (b *Bucket) Flush(timeLimit int64, force bool) bool {
	b.Downsample()
	// not all values may be allowed to be written to disk
	indexEnd := -1
	if timeLimit != math.MaxInt64 {
		indexEnd = b.Buffer.IndexOfTime(timeLimit)
	}

	if indexEnd == -1 {
		indexEnd = b.Buffer.Len()
	}

	if indexEnd == 0 {
		return false
	}

	fmt.Printf("flushing buffer %s with %d points\n", b.Path, b.Buffer.Len())

	// check file boundaries
	dataFile, created, count := b.GetStorageTime(b.Buffer.Values[0][:indexEnd])

	// hit file boundary, force flush
	if indexEnd != count {
		force = true
	}

	// transform values
	transformed := make([][]uint64, b.Buffer.Cols())

	for i, t := range b.Transformers {
		var err error
		transformed[i], err = t.Apply(b.Buffer.Values[i][:count])
		if err != nil {
			panic(err) // todo: make non-fatal
		}
	}

	// encode values
	var block bytes.Buffer
	header, err := encoding.EncodeBlock(&block, b.Buffer.Values[0][:count], transformed)

	if err != nil {
		panic(err) // todo: make non-fatal
	}

	// only write to disk if forced or the block can't fit any more values
	if !force && header.NumPoints == count {
		return false
	}

	// write transformed values to file
	err = dataFile.WriteBlock(block, false) // todo: fix overwrite and implement

	if err != nil {
		panic(err) // todo: make non-fatal
	}

	if created {
		b.DataFiles = append(b.DataFiles, dataFile)
		b.sortFiles()
	}

	fmt.Printf("wrote %d points to file, block size %d bytes", header.NumPoints, header.BytesUsed)

	b.LastTimeOnDisk = b.Buffer.Values[0][count-1]
	b.Buffer.Discard(header.NumPoints)

	b.LastFlush = time.Now()

	return true
}

func (b *Bucket) sortFiles() {
	sort.Slice(b.DataFiles, func(i, j int) bool {
		return b.DataFiles[i].TimeStart < b.DataFiles[j].TimeStart
	})
}

func (b *Bucket) loadFiles() error {
	b.DataFiles = make([]*storage.DataFile, 0, 16)

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
		file, err := storage.OpenDataFile(path.Join(b.Path, info.Name()), info, b.TimeStep*b.PointsPerFile)

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
	b.LastTimeOnDisk = math.MinInt64

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

	b.LastTimeOnDisk = header.TimeLast

	return nil
}

func OpenBucket(basePath string, timeStep int64, pointsPerFile int64) (Bucket, error) {
	b := Bucket{
		LastTimeOnDisk: math.MinInt64,
		TimeStep:       timeStep,
		PointsPerFile:  pointsPerFile,
		Path:           path.Join(basePath, strconv.FormatInt(timeStep, 10)),
		Dirty:          map[int64]struct{}{},
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

func (b *Bucket) getDataFile(fileTime int64) (file *storage.DataFile, created bool) {
	fileTime = util.RoundDown(fileTime, b.TimeStep*b.PointsPerFile)

	for j := range b.DataFiles {
		if b.DataFiles[j].TimeStart == fileTime {
			return b.DataFiles[j], false
		}
	}

	df := storage.NewDataFile(b.Path, fileTime, b.TimeStep*b.PointsPerFile)

	return df, true
}

// GetStorageTime checks how many points fit into the same file as the first point
// returns the number of points that fit and the time at which the file starts
func (b *Bucket) GetStorageTime(time []int64) (file *storage.DataFile, created bool, count int) {
	// find data file
	dataFile, created := b.getDataFile(time[0])

	// find all points that fit into this file
	for i, t := range time {
		if t > dataFile.TimeEnd {
			return dataFile, created, i
		}
	}

	return dataFile, created, len(time)
}

func (b *Bucket) WriteBlock(fileTime int64, buffer bytes.Buffer, overwrite bool) error {
	dataFile, created := b.getDataFile(fileTime)

	err := dataFile.WriteBlock(buffer, overwrite)

	if err != nil {
		return err
	}

	if created {
		b.DataFiles = append(b.DataFiles, dataFile)
		b.sortFiles()
	}

	return nil
}
