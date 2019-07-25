package main

import (
	"fmt"
	"log"
	"net/rpc"

	"github.com/martin2250/minitsdb/ingest"
	"github.com/martin2250/minitsdb/minitsdb"
)

var session minitsdb.ServerSession

func main() {
	var err error
	session, err = minitsdb.LoadDatabase("/home/martin/go/src/github.com/martin2250/minitsdb/database")

	if err != nil {
		panic(err)
	}

	err = session.LoadSeries()

	if err != nil {
		panic(err)
	}

	client, err := rpc.DialHTTP("tcp", "localhost:2002")

	if err != nil {
		log.Fatal(err)
	}

	var point ingest.Point
	err = client.Call("Buffer.PopPoint", 0, &point)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(point)

	// http.HandleFunc("/insert", handleInsert)

	// http.ListenAndServe(":8080", nil)
}
