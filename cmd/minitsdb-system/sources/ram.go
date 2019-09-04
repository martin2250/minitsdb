package sources

import (
	"github.com/shirou/gopsutil/mem"
)

type RAM struct {
	Buffered bool
	Factor   float64
}

func (s *RAM) Variables() ([]string, error) {
	cols := []string{
		"name:ram stat:used",
		"name:ram stat:buffer",
	}

	if !s.Buffered {
		return cols[:1], nil
	}

	return cols, nil
}

func (s *RAM) Read() ([]float64, error) {
	vm, err := mem.VirtualMemory()

	if err != nil {
		return nil, err
	}

	values := []float64{
		float64(vm.Used) / s.Factor,
		float64(vm.Buffers) / s.Factor,
	}

	if !s.Buffered {
		return values[:1], nil
	}
	return values, nil
}
