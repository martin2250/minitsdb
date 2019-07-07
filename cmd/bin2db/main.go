package main

import (
	"encoding/binary"
	"log"
	"os"

	"github.com/martin2250/minitsdb/encoder"
)

func readDataBin() (<-chan [13]int64, error) {
	out := make(chan [13]int64, 500)

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
		log.Fatal(err)
	}

	values := make([][]int64, 13)

	for i := range values {
		values[i] = make([]int64, 0)
	}

	channelOpen := true

	f, err := os.Create("data.base")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for channelOpen || len(values[0]) > 0 {
		for channelOpen && len(values[0]) < 400 {
			point, ok := <-c
			if ok {
				for i, v := range point {
					values[i] = append(values[i], v)
				}
			} else {
				channelOpen = false
			}
		}

		buffer, count, err := encoder.EncodeBlock(values)

		log.Printf("%d points\n", count)

		if err != nil {
			log.Fatal(err)
		}

		for i := range values {
			values[i] = values[i][count:]
		}

		if _, err = buffer.WriteTo(f); err != nil {
			log.Fatal(err)
		}
	}
}
