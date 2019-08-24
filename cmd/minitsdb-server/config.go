package main

import (
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"os"
	"time"
)

type Configuration struct {
	DatabasePath string

	ApiListenAddress string
	ApiPath          string
	ApiTimeout       time.Duration

	TcpListenAddress string

	IngestBufferCapacity int

	IngestionWorkerCount uint

	ShutdownTimeout time.Duration

	Telegram *struct {
		AppName   string
		AuthToken string
		ChatID    string
	}
}

// readConfigurationFile does what the name implies
// kills the application when there is an error
func readConfigurationFile(confpath string) Configuration {
	opts := Configuration{
		DatabasePath: "",

		ApiListenAddress: ":8080",
		ApiPath:          "/api/",
		ApiTimeout:       10 * time.Second,

		TcpListenAddress:     ":8081",
		IngestBufferCapacity: 8192,

		IngestionWorkerCount: 1,

		ShutdownTimeout: 5 * time.Second,
	}

	if confpath == "" {
		return opts
	}

	log.WithField("path", confpath).Info("Loading configuration file")

	f, err := os.Open(confpath)
	if err != nil {
		log.WithError(err).Fatal("could not open configuration file")
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	err = dec.Decode(&opts)
	if err != nil {
		log.WithError(err).Fatal("could not parse configuration file")
	}

	return opts
}
