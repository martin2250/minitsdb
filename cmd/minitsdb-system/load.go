package main

import (
	"github.com/martin2250/minitsdb/cmd/minitsdb-system/sources"
	"gopkg.in/yaml.v3"
	"log"
)

func loadSources(sourceConfs []yaml.Node) []sources.Source {
	s := make([]sources.Source, len(sourceConfs))

	for i, node := range sourceConfs {
		t := struct {
			Type string
		}{}

		err := node.Decode(&t)

		if err != nil {
			log.Fatalf("error while parsing source %s", err.Error())
		}

		gen, ok := sources.Sources[t.Type]

		if !ok {
			log.Fatalf("source %s unknown", t.Type)
		}

		s[i], err = gen(node)

		if err != nil {
			log.Fatalf("error while parsing source %s", err.Error())
		}
	}

	return s
}
