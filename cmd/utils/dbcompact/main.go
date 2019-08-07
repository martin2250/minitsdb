package main

import (
	"flag"
	"fmt"
	"github.com/martin2250/minitsdb/database/series/storage/encoding"
	"io"
	"log"
	"os"

	"github.com/martin2250/minitsdb/database/series/storage"
)

var opts struct {
	pathInput  string
	pathOutput string

	sizeBuffer int
}

func init() {
	flag.StringVar(&opts.pathInput, "input", "../../data.base", "input file")
	flag.StringVar(&opts.pathOutput, "output", "test.base", "output file")
	flag.IntVar(&opts.sizeBuffer, "buffer", 5000, "number of points to buffer before trying to write a new block")

	flag.Parse()
}

func check(err error) {
	if err != nil {
		log.Fatalln(err)
	}
}

func main() {
	fileInput, err := os.Open(opts.pathInput)
	check(err)
	defer fileInput.Close()

	fileOutput, err := os.Create(opts.pathOutput)
	check(err)
	defer fileOutput.Close()

	_, values, err := storage.ReadBlock(fileInput)

	if err != nil {
		log.Fatalf("First Block damaged: %v\n", err)
	}

	available := true
	blockCounterInput := 0
	blockCounterOutput := 0
	pointCounter := 0

	for available || len(values[0]) > 0 {
		if available && len(values[0]) < opts.sizeBuffer {
			blockCounterInput++
			header, valuesNew, err := storage.ReadBlock(fileInput)

			if err == io.EOF {
				available = false
				continue
			} else if err != nil {
				log.Printf("Error reading block #%d %v\n", blockCounterInput, err)
				continue
			}

			if len(valuesNew) != len(values) {
				log.Printf("Block #%d has different column count from previous, skipping (%d, %d)\n", blockCounterInput, len(valuesNew), len(values))
				continue
			}

			for i, column := range valuesNew {
				values[i] = append(values[i], column...)
			}

			pointCounter += int(header.NumPoints)

			continue
		}

		buffer, count, err := encoding.EncodeBlock(values)

		if err != nil {
			log.Fatalf("Error encoding block %v\n", err)
		}

		n, err := buffer.WriteTo(fileOutput)

		if n != 4096 || err != nil {
			log.Fatalf("Error writing to output file (%d bytes written) %v\n", n, err)
		}

		for i := range values {
			values[i] = values[i][count:]
		}

		blockCounterOutput++
	}

	fmt.Printf("Compacted %d points from %d into %d block\n", pointCounter, blockCounterInput, blockCounterOutput)
}
