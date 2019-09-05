package main

import (
	"gopkg.in/yaml.v3"
	"log"
	"os"
	"time"
)

type Configuration struct {
	Series map[string]string

	Address  string
	Protocol string

	Sources []yaml.Node

	Interval time.Duration
}

func readConfigurationFile(confpath string) Configuration {
	conf := Configuration{}

	f, err := os.Open(confpath)
	if err != nil {
		log.Fatal("could not open configuration file")
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	err = dec.Decode(&conf)
	if err != nil {
		log.Fatal("could not parse configuration file")
	}

	return conf
}
