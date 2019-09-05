package sources

import (
	"errors"
	"gopkg.in/yaml.v3"
)

type Source interface {
	Read() ([]string, error)
	Init() error
}

type SourceGenerator func(node yaml.Node) (Source, error)

var Sources = map[string]SourceGenerator{}

var ErrNotReady = errors.New("source not ready")

func init() {
	Sources["cpu"] = func(node yaml.Node) (source Source, e error) {
		cpu := CPU{}

		err := node.Decode(&cpu)

		if err != nil {
			return nil, err
		}

		return &cpu, nil
	}
}

func init() {
	Sources["file"] = func(node yaml.Node) (source Source, e error) {
		file := File{
			Factor: 1.0,
		}

		err := node.Decode(&file)

		if err != nil {
			return nil, err
		}

		return &file, nil
	}
}

func init() {
	Sources["ram"] = func(node yaml.Node) (source Source, e error) {
		ram := struct {
			Buffered bool
		}{}

		err := node.Decode(&ram)

		if err != nil {
			return nil, err
		}

		return &RAM{
			Buffered: ram.Buffered,
		}, nil
	}
}
