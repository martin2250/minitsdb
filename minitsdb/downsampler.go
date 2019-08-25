package minitsdb

import (
	"github.com/martin2250/minitsdb/minitsdb/downsampling"
	"github.com/martin2250/minitsdb/minitsdb/storage"
)

type Downsampler struct {
	Series *Series
	Source PointSource
	Sink   *storage.Bucket
}

func Downsample(s *Series, src PointSource, dst *storage.Bucket) error {

}

func (s *Series) DownsampleFirst() error {
	queryColumns := make([]QueryColumn, 0)
	for columnIndex, c := range s.Columns {
		for aggregatorIndex, secondaryIndex := range c.IndexSecondary {
			if secondaryIndex != 0 {
				queryColumns = append(queryColumns, QueryColumn{
					Column:   &s.Columns[columnIndex],
					Function: downsampling.AggregatorList[aggregatorIndex],
				})
			}
		}
	}
}
