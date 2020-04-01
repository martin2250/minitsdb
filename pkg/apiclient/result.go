package apiclient

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

type Series struct {
	Tags    map[string]string
	Columns []map[string]string
}

type QueryResult struct {
	Series []Series
	r      *bufio.Reader
}

type QueryChunk struct {
	Index  int
	Times  []int64
	Values [][]float64
}

type chunkDescription struct {
	SeriesIndex int
	NumValues   int
	NumPoints   int
}

func (r *QueryResult) readDescription() (chunkDescription, error) {
	buf, err := r.r.ReadBytes('\n')
	if err != nil {
		return chunkDescription{}, err
	}
	var desc chunkDescription
	err = json.Unmarshal(buf, &desc)
	return desc, err
}

func (r *QueryResult) ReadChunk() (QueryChunk, error) {
	desc, err := r.readDescription()
	if err != nil {
		return QueryChunk{}, err
	}
	if desc.SeriesIndex >= len(r.Series) {
		return QueryChunk{}, errors.New("series index out of range")
	}
	if desc.NumValues != len(r.Series[desc.SeriesIndex].Columns) {
		return QueryChunk{}, errors.New("wrong number of columns")
	}
	c := QueryChunk{
		Index:  desc.SeriesIndex,
		Times:  make([]int64, desc.NumPoints),
		Values: make([][]float64, desc.NumValues),
	}
	err = binary.Read(r.r, binary.LittleEndian, &c.Times)
	if err != nil {
		return QueryChunk{}, err
	}
	for i := range c.Values {
		c.Values[i] = make([]float64, desc.NumPoints)
		err = binary.Read(r.r, binary.LittleEndian, &c.Values[i])
		if err != nil {
			return QueryChunk{}, err
		}
	}
	return c, nil
}

func (c *QueryChunk) Append(o QueryChunk) error {
	if len(c.Values) != len(o.Values) {
		return errors.New("numbers of columns don't match")
	}
	c.Times = append(c.Times, o.Times...)
	for i := range c.Values {
		c.Values[i] = append(c.Values[i], o.Values[i]...)
	}
	return nil
}

func (r *QueryResult) ReadAll() ([]QueryChunk, error) {
	chunks := make([]QueryChunk, len(r.Series))
	for i := range chunks {
		chunks[i].Index = i
		chunks[i].Values = make([][]float64, len(r.Series[i].Columns))
	}
	for {
		c, err := r.ReadChunk()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		err = chunks[c.Index].Append(c)
		if err != nil {
			return nil, err
		}
	}
	return chunks, nil
}
