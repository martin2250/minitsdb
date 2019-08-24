package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/martin2250/minitsdb/minitsdb/storage/encoding"
	"io"
	"log"
	"os"
	"path"
)

var opts struct {
	pathInput  string
	pathOutput string

	sizeBuffer int
	pointsFile int64
}

func init() {
	flag.StringVar(&opts.pathInput, "input", "../../data.bin", "input file")
	flag.StringVar(&opts.pathOutput, "output", "/tmp/database", "output directory")
	flag.IntVar(&opts.sizeBuffer, "buffer", 5000, "number of points to buffer before trying to write a new block")
	flag.Int64Var(&opts.pointsFile, "pointsfile", 24*3600, "number of points per file")

	flag.Parse()
}

type binaryData struct {
	file *os.File
}

func openBinary(path string) (bd binaryData, err error) {
	bd.file, err = os.Open(opts.pathInput)
	return
}

func (bd binaryData) ReadPoint() (values []int64, err error) {
	values = make([]int64, 13)

	var time int64

	err = binary.Read(bd.file, binary.BigEndian, &time)

	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return
	}

	var val32 [12]int32

	err = binary.Read(bd.file, binary.BigEndian, &val32)

	if err != nil {
		return
	}

	values[0] = time

	for i := range val32 {
		values[i+1] = int64(val32[i])
	}

	return
}

func getFileName(time int64) string {
	return path.Join(opts.pathOutput, fmt.Sprintf("%011d.mdb", time))
}

func appendMany(values [][]int64, valuesNew []int64) {
	for i := range values {
		values[i] = append(values[i], valuesNew[i])
	}
}

func main() {
	bd, err := openBinary(opts.pathInput)

	if err != nil {
		log.Fatal(err)
	}

	if err := os.MkdirAll(opts.pathOutput, 0755); err != nil {
		log.Fatalln(err)
	}

	values := make([][]int64, 13)
	valuesNew, err := bd.ReadPoint()

	if err != nil {
		log.Fatalln(err)
	}

	for i := range values {
		values[i] = make([]int64, 1)
		values[i][0] = valuesNew[i]
	}

	var outputFile *os.File
	var fileStartTime = (valuesNew[0] / opts.pointsFile) * opts.pointsFile
	var flush bool
	var eof bool

	for !eof {
		for !flush && len(values[0]) < opts.sizeBuffer {
			valuesNew, err = bd.ReadPoint()

			if err == io.EOF {
				flush = true
				eof = true
			}

			if err != nil {
				log.Fatalln(err)
			}

			if valuesNew[0] >= (fileStartTime + opts.pointsFile) {
				flush = true
				break
			}

			appendMany(values, valuesNew)
			valuesNew = nil
		}

		if outputFile == nil {
			if len(values[0]) > 0 {
				log.Printf("create new file\n")
				outputFile, err = os.Create(getFileName(fileStartTime))

				if err != nil {
					log.Fatal(err)
				}
			} else {
				log.Printf("skip empty file\n")
				fileStartTime += opts.pointsFile
				continue
			}
		}

		for (flush && len(values[0]) > 0) || len(values[0]) >= opts.sizeBuffer {
			buffer, count, err := encoding.EncodeBlock(values)

			if count == 1 {
				log.Printf("vals: %v", values)
			}

			if err != nil {
				log.Fatal(err)
			}

			for i := range values {
				values[i] = values[i][count:]
			}

			if _, err = buffer.WriteTo(outputFile); err != nil {
				log.Fatal(err)
			}
		}

		if flush {
			flush = false
			fileStartTime += opts.pointsFile

			if !eof && valuesNew != nil {
				appendMany(values, valuesNew)
				for fileStartTime < (valuesNew[0]/opts.pointsFile)*opts.pointsFile {
					fileStartTime += opts.pointsFile
				}
				valuesNew = nil
			}

			outputFile.Close()
			outputFile = nil
		}
	}
}
