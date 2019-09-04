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

func (f *File) Variables() ([]string, error) {
	if _, ok := f.Tags["name"]; !ok {
		return nil, errors.New("name tag must be present")
	}

	tags := make([]string, 0, len(f.Tags))

	for k, v := range f.Tags {
		tags = append(tags, fmt.Sprintf("%s:%s", k, v))
	}

	desc := strings.Join(tags, " ")

	return []string{desc}, nil
}

func (f *File) Read() ([]float64, error) {
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

	value, err := strconv.ParseFloat(s, 64)

	if err != nil {
		return nil, err
	}

	value *= f.Factor

	return []float64{value}, nil
}
