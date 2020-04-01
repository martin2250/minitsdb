package main

import (
	"github.com/martin2250/minitsdb/pkg/apiclient"
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"time"
)

type WatchConfig struct {
	Series map[string]string
	Column apiclient.Column

	TimeRange time.Duration
	TimeStep  time.Duration

	Range *struct {
		Min float64
		Max float64
	}

	MinPoints *int

	Interval time.Duration
}

type Configuration struct {
	Address string

	Watches []WatchConfig

	Telegram struct {
		ApiToken string
		ChatID   int64
	}
}

func readConfigurationFile(confpath string) Configuration {
	conf := Configuration{}

	f, err := os.Open(confpath)
	if err != nil {
		log.Fatalf("could not open configuration file %v", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	err = dec.Decode(&conf)
	if err != nil {
		log.Fatalf("could not parse configuration file %v", err)
	}

	return conf
}
