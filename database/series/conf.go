package series

import (
	"errors"
	"os"
	"path"
	"time"

	"gopkg.in/yaml.v2"
)

// YamlBucketConfig describes a downsampling bucket in SeriesConfig
type YamlBucketConfig struct {
	Factor int
}

// YamlColumnConfig describes a column group (duplicate not applied yet) in SeriesConfig
type YamlColumnConfig struct {
	Decimals    int
	Tags        map[string]string
	Duplicate   []map[string]string
	Transformer string
}

// YamlSeriesConfig describes the YAML file for a series
type YamlSeriesConfig struct {
	FlushInterval time.Duration

	FlushCount      int
	ForceFlushCount int

	ReuseMax   int
	PointsFile int64

	Tags map[string]string

	Buckets []YamlBucketConfig
	Columns []YamlColumnConfig
}

// LoadSeriesYamlConfig loads SeriesConfig from yaml file
func LoadSeriesYamlConfig(seriesPath string) (YamlSeriesConfig, error) {
	f, err := os.Open(path.Join(seriesPath, "series.yaml"))
	if err != nil {
		return YamlSeriesConfig{}, err
	}

	defer f.Close()

	d := yaml.NewDecoder(f)
	d.SetStrict(true)

	c := YamlSeriesConfig{}
	err = d.Decode(&c)

	if err != nil {
		return YamlSeriesConfig{}, err
	}

	err = c.Check()

	if err != nil {
		return YamlSeriesConfig{}, err
	}

	return c, nil
}

// Check checks config for errors
func (c *YamlSeriesConfig) Check() error {
	if c.FlushInterval < 10*time.Second {
		return errors.New("flush interval must be greater than or equal to 10s")
	}

	if c.FlushCount < 50 {
		return errors.New("flush count must be greater than 50 or equal to points")
	}

	if c.ForceFlushCount < c.FlushCount {
		return errors.New("force flush count must be greater than or equal to flush count")
	}

	if c.ReuseMax < 0 || c.ReuseMax > 4096 {
		return errors.New("reusemax must be between 0 and 4096 bytes")
	}

	if c.PointsFile < 1000 {
		return errors.New("pointsfile must be between greater than or equal to 1000")
	}

	if _, ok := c.Tags["name"]; !ok {
		return errors.New("series tag set must contain 'name'")
	}

	if len(c.Buckets) == 0 {
		return errors.New("no buckets declared")
	}

	for i, b := range c.Buckets {
		if (i != 0 && b.Factor < 2) || b.Factor < 1 {
			return errors.New("bucket downsampling factor must be greater than one")
		}
	}

	for _, col := range c.Columns {
		// todo: maybe remove this restriction
		if col.Decimals > 10 || col.Decimals < -10 {
			return errors.New("column must have between -10 and 10 decimals")
		}

		if _, ok := col.Tags["name"]; !ok {
			return errors.New("column tag set must contain 'name'")
		}

		if _, ok := col.Tags["aggregation"]; ok {
			return errors.New("column tag set may not contain 'aggregation'")
		}
	}

	return nil
}
