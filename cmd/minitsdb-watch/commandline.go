package main

import (
	"github.com/jessevdk/go-flags"
	"log"
)

type CommandLineOptions struct {
	ConfigPath string `short:"c" long:"config" description:"configuration file"`
}

func readCommandLineOptions() CommandLineOptions {
	opts := CommandLineOptions{}
	_, err := flags.Parse(&opts)

	if err != nil {
		log.Fatal("error while parsing command line options")
	}

	return opts
}
