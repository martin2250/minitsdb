package main

import (
	"github.com/martin2250/minitsdb/cmd/minitsdb-server/api"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"math"
	"os"
	"path"
	"time"
)

type confIngest struct {
	Servers []string
	Buffer  int
}

type Configuration struct {
	DatabasePath string

	API api.Config

	Ingest confIngest

	ShutdownTimeout time.Duration

	Logging struct {
		Telegram *struct {
			AppName   string
			AuthToken string
			ChatID    string
		}
	}
}

var (
	ConfigDefault = Configuration{
		API: api.Config{
			ServeTimeout: 5 * time.Second,
			WaitTimeout:  20 * time.Millisecond,
			MaxPoints:    math.MaxInt64,
		},
		Ingest: confIngest{
			Buffer: 1024,
		},
		ShutdownTimeout: 5 * time.Second,
	}
	ConfigNoConfig = Configuration{
		DatabasePath: "",
		API: api.Config{
			Address:      ":8080",
			ServeTimeout: 5 * time.Second,
			WaitTimeout:  20 * time.Millisecond,
			MaxPoints:    math.MaxInt64,
		},
		Ingest: confIngest{
			Buffer: 16,
		},
		ShutdownTimeout: 5 * time.Second,
	}
)

// readConfigurationFile does what the name implies
// kills the application when there is an error
func readConfigurationFile(confpath string) Configuration {
	conf := ConfigDefault

	logrus.WithField("path", confpath).Info("loading configuration file")

	f, err := os.Open(confpath)
	if err != nil {
		logrus.WithError(err).Fatal("could not open configuration file")
	}
	defer f.Close()

	err = yaml.NewDecoder(f).Decode(&conf)
	if err != nil {
		logrus.WithError(err).Fatal("could not parse configuration file")
	}

	if !path.IsAbs(conf.DatabasePath) {
		conf.DatabasePath = path.Join(path.Dir(confpath), conf.DatabasePath)
	}

	return conf
}
