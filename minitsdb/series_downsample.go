package minitsdb

import (
	"bytes"
	"errors"
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"io"
	"math"
)

type Downsampler struct {
	source       BucketQuerySource
	dst          *storage.Bucket
	transformers []encoding.Transformer
	buffer       [][]int64
}

func NewDownsampler(s *Series, src, dst *storage.Bucket, primary bool) Downsampler {
	// -1 because we don't need time
	queryColumns := make([]QueryColumn, 1, s.SecondaryCount-1)
	queryColumns[0] = QueryColumn{
		Column:   &s.Columns[0],
		Function: downsampling.Count,
	}

	transformers := make([]encoding.Transformer, 2, s.SecondaryCount)
	transformers[0] = encoding.TimeTransformer
	transformers[1] = encoding.CountTransformer

	for indexColumn, c := range s.Columns {
		for indexAggregator, indexSecondary := range c.IndexSecondary {
			if indexSecondary > 1 {
				queryColumns = append(queryColumns, QueryColumn{
					Column:   &s.Columns[indexColumn],
					Function: downsampling.AggregatorList[indexAggregator],
				})
				transformers = append(transformers, c.Transformer)
			}
		}
	}

	timeRange := TimeRange{
		Start: dst.TimeLast,
		End:   math.MaxInt64 - 100*dst.TimeResolution, // prevent overflows
	}

	querySource := NewBucketQuerySource(s, src, &timeRange, queryColumns, dst.TimeResolution, primary)

	d := Downsampler{
		source:       querySource,
		transformers: transformers,
		buffer:       make([][]int64, s.SecondaryCount),
		dst:          dst,
	}

	return d
}

func (d *Downsampler) ReadBlock() error {
	newValues, err := d.source.Next()

	if err != nil {
		return err
	}

	if newValues.Cols() != len(d.buffer)-1 {
		return errors.New("number of columns doesn't match")
	}

	d.buffer[0] = append(d.buffer[0], newValues.Time...)
	for i := range newValues.Values {
		d.buffer[i+1] = append(d.buffer[i+1], newValues.Values[i]...)
	}

	return nil
}

var ErrBlockNotFull = errors.New("block could not be filled completely")

func (d *Downsampler) WriteBlock() error {
	if len(d.buffer[0]) == 0 {
		return ErrBlockNotFull
	}

	dataFile, count := d.dst.GetStorageTime(d.buffer[0])

	transformed := make([][]uint64, len(d.buffer))
	var err error
	for i, t := range d.transformers {
		transformed[i], err = t.Apply(d.buffer[i][:count])
		if err != nil {
			return err
		}
	}

	var block bytes.Buffer
	header, err := encoding.EncodeBlock(&block, d.buffer[0][:count], transformed)

	if err != nil {
		return err
	}

	if header.NumPoints == len(d.buffer[0]) {
		return ErrBlockNotFull
	}

	err = dataFile.WriteBlock(block, false)

	if err != nil {
		return err
	}

	d.dst.TimeLast = d.buffer[0][count-1]

	for i := range d.buffer {
		length := copy(d.buffer[i], d.buffer[i][header.NumPoints:])
		d.buffer[i] = d.buffer[i][:length]
	}

	return nil
}

func (d *Downsampler) Run() error {
	for {
		errRead := d.ReadBlock()

		if errRead != io.EOF && errRead != nil {
			return errRead
		}

		var errWrite error
		for errWrite == nil {
			errWrite = d.WriteBlock()
		}

		if errWrite == ErrBlockNotFull {
			if errRead == io.EOF {
				return nil
			}

		} else if errWrite != nil {
			return errWrite
		}
	}
}
