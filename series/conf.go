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
	Decimals  int
	Tags      map[string]string
	Duplicate []map[string]string
}

// YamlSeriesConfig describes the YAML file for a series
type YamlSeriesConfig struct {
	FlushDelay time.Duration
	Buffer     int
	ReuseMax   int

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
	if c.FlushDelay < 10*time.Second {
		return errors.New("flush delay must be greater than 10s")
	}

	if c.Buffer < 50 {
		return errors.New("buffer size must be greater than 50 points")
	}

	if c.ReuseMax < 0 || c.ReuseMax > 4096 {
		return errors.New("reusemax must be between 0 and 4096 bytes")
	}

	if _, ok := c.Tags["name"]; !ok {
		return errors.New("series tag set must contain 'name'")
	}

	for _, b := range c.Buckets {
		if b.Factor < 2 {
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
