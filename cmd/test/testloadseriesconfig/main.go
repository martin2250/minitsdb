package main

import (
	"log"

	"github.com/martin2250/minitsdb/series"
)

func main() {

	s, err := series.OpenSeries("../../database/power.main")

	if err != nil {
		log.Fatalln(err)
	}

	log.Printf("%+v\n", s)
}
