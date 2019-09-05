package sources

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type File struct {
	Path   string
	Factor float64

	Tags map[string]string
}

func (f *File) Init() error {
	if _, ok := f.Tags["name"]; !ok {
		return errors.New("name tag must be present")
	}
	return nil
}

func (f *File) Read() ([]string, error) {
	file, err := os.Open(f.Path)

	if err != nil {
		return nil, err
	}

	defer file.Close()

	data := make([]byte, 128)

	n, err := file.Read(data)

	if err != nil && err != io.EOF {
		return nil, err
	}

	s := string(data[:n])
	s = strings.TrimSpace(s)

	if f.Factor != 1.0 {
		value, err := strconv.ParseFloat(s, 64)

		if err != nil {
			return nil, err
		}

		s = strconv.FormatFloat(value*f.Factor, 'g', -1, 64)
	}

	point := ""

	for k, v := range f.Tags {
		point += fmt.Sprintf("%s:%s ", k, v)
	}

	point += s

	return []string{point}, nil
}
