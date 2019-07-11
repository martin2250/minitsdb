package main

import (
	"log"

	"github.com/martin2250/minitsdb/series"
)

func main() {

	c, err := series.LoadSeriesYamlConfig("../../database/power.main")
	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%+v\n", c)
}
