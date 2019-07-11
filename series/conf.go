package series

import (
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
func LoadSeriesYamlConfig(seriesPath string) (c YamlSeriesConfig, err error) {
	f, err := os.Open(path.Join(seriesPath, "series.yaml"))
	if err != nil {
		return
	}
	defer f.Close()

	d := yaml.NewDecoder(f)
	d.SetStrict(true)

	err = d.Decode(&c)
	if err != nil {
		return
	}

	return c, err
}
