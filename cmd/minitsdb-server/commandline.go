package main

import (
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"
	"os"
)

type CommandLineOptions struct {
	DatabasePath string `short:"d" long:"database" description:"database path"`
	ConfigPath   string `short:"c" long:"config" description:"configuration file"`

	CpuProfilePath string `long:"cpuprofile" description:"record a cpu profile to this file"`
	CpuProfilePlot bool   `long:"plot" description:"plot the recorded cpu profile"`

	TracePath string `long:"trace" description:"record a trace to this file"`
}

func readCommandLineOptions() CommandLineOptions {
	opts := CommandLineOptions{}
	_, err := flags.Parse(&opts)

	switch errt := err.(type) {
	case *flags.Error:
		if errt.Type == flags.ErrHelp {
			os.Exit(0)
		}
	}

	if err != nil {
		log.WithError(err).Fatal("could not parse command line arguments")
	}

	return opts
}
