package main

import (
	"github.com/jessevdk/go-flags"
	"log"
)

type CommandLineOptions struct {
	ConfigPath string `short:"c" long:"config" description:"configuration file"`
	Stdout     bool   `long:"stdout" description:"send output to stdout instead of sink"`
}

func readCommandLineOptions() CommandLineOptions {
	opts := CommandLineOptions{}
	_, err := flags.Parse(&opts)

	if err != nil {
		log.Fatal("error while parsing command line options")
	}

	return opts
}
