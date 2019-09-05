package main

import (
	"container/list"
	"github.com/jessevdk/go-flags"
	"net/http"
)

func main() {
	opts := struct {
		TcpAddr  string `short:"t" long:"tcp" description:"tcp listener address"`
		UdpAddr  string `short:"u" long:"udp" description:"udp listener address"`
		HttpAddr string `short:"a" long:"api" description:"http api address" required:"true"`
	}{}

	_, err := flags.Parse(&opts)

	if err != nil {
		return
	}

	buf := IngestBuffer{
		Points: list.New(),
		Errors: list.New(),
	}

	if opts.TcpAddr != "" {
		go func() {
			err := buf.ListenTCP(opts.TcpAddr)
			if err != nil {
				panic(err)
			}
		}()
	}

	if opts.UdpAddr != "" {
		go func() {
			err := buf.ListenUDP(opts.UdpAddr)
			if err != nil {
				panic(err)
			}
		}()
	}

	go buf.CleanErrors()

	http.HandleFunc("/insert", func(w http.ResponseWriter, r *http.Request) { buf.ServeHTTPPut(w, r) })
	http.HandleFunc("/read", func(w http.ResponseWriter, r *http.Request) { buf.ServeHTTPRead(w, r, true) })
	http.HandleFunc("/peek", func(w http.ResponseWriter, r *http.Request) { buf.ServeHTTPRead(w, r, false) })
	http.HandleFunc("/errors", func(w http.ResponseWriter, r *http.Request) { buf.ServeHTTPErrors(w, r) })

	http.ListenAndServe(opts.HttpAddr, nil)
}
