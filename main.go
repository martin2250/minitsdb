package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"

	"github.com/jwilder/encoding/simple8b"
	_ "github.com/jwilder/encoding/simple8b"
)

func readDataBin() (<-chan [13]int64, error) {
	out := make(chan [13]int64)

	go func() {
		file, err := os.Open("data.bin")

		if err != nil {
			close(out)
			return
		}

		var point struct {
			Time   int64
			Values [12]int32
		}

		for {
			err = binary.Read(file, binary.BigEndian, &point)
			if err != nil {
				close(out)
				file.Close()
				return
			}

			var buffer [13]int64
			buffer[0] = point.Time

			for i, v := range point.Values {
				buffer[i+1] = int64(v)
			}
			out <- buffer
		}
	}()

	return out, nil
}

func main() {
	c, err := readDataBin()

	if err != nil {
		log.Fatalf("%v", err)
	}

	const N = 513271

	var values [13][N]uint64
	var last [13]int64

	for ival := 0; ival < N; ival++ {
		for ivar, val := range <-c {
			values[ivar][ival] = zigZag(val - last[ivar])
			last[ivar] = val
		}
	}

	buf := new(bytes.Buffer)

	for ivar := 0; ivar < 13; ivar++ {
		buffer, err := simple8b.EncodeAll(values[ivar][:])

		if err != nil {
			log.Fatalf("%v", err)
		}

		if err = binary.Write(buf, binary.LittleEndian, buffer); err != nil {
			log.Fatalf("%v", err)
		}
	}

	fmt.Printf("%v\n", len(buf.Bytes()))
	// fmt.Printf("%v\n", values)

}
