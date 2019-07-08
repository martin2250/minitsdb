package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/martin2250/minitsdb/encoder"
)

func query(writer http.ResponseWriter, from int64, to int64) error {
	file, err := os.Open("../../data.base")
	defer file.Close()

	if err != nil {
		return err
	}

	for {
		buffer := make([]byte, 4096)

		if _, err := io.ReadFull(file, buffer); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		r := bytes.NewReader(buffer)

		header, err := encoder.DecodeHeader(r)

		if err != nil {
			return err
		}

		if header.TimeLast < from {
			continue
		}
		if header.TimeFirst > to {
			break
		}

		r.Reset(buffer)

		header, values, err := encoder.DecodeBlock(r)
		// _, _, err = encoder.DecodeBlock(r)

		if err != nil {
			return err
		}

		buf := new(bytes.Buffer)

		for i := range values {
			if err := binary.Write(buf, binary.LittleEndian, values[i]); err != nil {
				return err
			}
		}

		buf.WriteTo(writer)

		// for j := range values[0] {
		// 	if values[0][j] >= from && values[0][j] <= to {
		// 		for i := range values {
		// 			io.WriteString(writer, strconv.FormatInt(values[i][j], 10))
		// 			writer.Write([]byte{' '})
		// 		}
		// 		writer.Write([]byte{'\n'})
		// 	}
		// }
	}

	return nil
}

func handleQuery(writer http.ResponseWriter, request *http.Request) {
	var from, to int64

	_, err := fmt.Sscanf(request.URL.Path, "/query/%d/%d", &from, &to)

	writer.WriteHeader(200)

	if err != nil {
		log.Printf("%v, %v", request.URL, err)
		io.WriteString(writer, fmt.Sprintf("shit gone south!\n%v\n", err))
		return
	}

	log.Printf("from: %d, to: %d\n", from, to)

	err = query(writer, from, to)

	if err != nil {
		log.Printf("%v, %v", request.URL, err)
		io.WriteString(writer, fmt.Sprintf("shit gone south!\n%v\n", err))
		return
	}

	log.Printf("served\n")
}

const port = 8080

func main() {
	http.HandleFunc("/query/", handleQuery)

	http.ListenAndServe(":8080", nil)
}
