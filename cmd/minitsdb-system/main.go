package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

func main() {
	opts := readCommandLineOptions()

	conf := readConfigurationFile(opts.ConfigPath)

	sources := loadSources(conf.Sources)

	conn, err := net.DialTimeout("tcp", conf.Address, 500*time.Millisecond)

	if err != nil {
		panic(err)
	}

	{
		var tags string
		for k, v := range conf.Series {
			tags += " " + k + ":" + v
		}

		_, err = fmt.Fprintf(conn, "SERIES%s\n", tags)
		if err != nil {
			panic(err)
		}
	}

	{
		var columns []string
		for _, s := range sources {
			cols, err := s.Variables()

			if err != nil {
				panic(err)
			}

			columns = append(columns, cols...)
		}

		_, err = fmt.Fprintf(conn, "COLUMNS %s\n", strings.Join(columns, "|"))
		if err != nil {
			panic(err)
		}
	}

	timer := time.NewTicker(conf.Interval)

	for {
		<-timer.C

		var variables []string
		for _, s := range sources {
			vars, err := s.Read()
			if err != nil {
				panic(err)
			}
			for _, v := range vars {
				variables = append(variables, strconv.FormatFloat(v, 'g', -1, 64))
			}
		}

		_, err = fmt.Fprintf(conn, "POINT %s %d\n", strings.Join(variables, " "), time.Now().Unix())
		if err != nil {
			panic(err)
		}
	}
}
