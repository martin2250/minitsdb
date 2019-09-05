package main

import (
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	opts := readCommandLineOptions()

	conf := readConfigurationFile(opts.ConfigPath)

	sources := loadSources(conf.Sources)

	var err error
	var w io.Writer
	if opts.Stdout {
		w = os.Stdout
	} else {
		switch conf.Protocol {
		case "tcp", "udp":
			w, err = net.DialTimeout(conf.Protocol, conf.Address, 500*time.Millisecond)
		default:
			panic("unknown protocol")
		}
	}
	if err != nil {
		panic(err)
	}

	for _, s := range sources {
		if err = s.Init(); err != nil {
			panic(err)
		}
	}

	for timestamp := range time.NewTicker(conf.Interval).C {
		sb := strings.Builder{}

		for k, v := range conf.Series {
			sb.WriteString(k)
			sb.WriteByte(':')
			sb.WriteString(v)
			sb.WriteByte(' ')
		}
		sb.WriteByte('|')

		for _, s := range sources {
			vars, err := s.Read()
			if err != nil {
				panic(err)
			}
			for _, v := range vars {
				sb.WriteString(v)
				sb.WriteByte('|')
			}
		}
		sb.WriteString(strconv.FormatInt(timestamp.Unix(), 10))
		sb.WriteByte('\n')

		_, err = w.Write([]byte(sb.String()))
		if err != nil {
			panic(err)
		}
	}
}
