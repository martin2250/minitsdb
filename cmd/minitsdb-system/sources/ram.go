package sources

import (
	"fmt"
	"github.com/shirou/gopsutil/mem"
)

type RAM struct {
	Buffered bool
}

func (s *RAM) Init() error {
	return nil
}

func (s *RAM) Read() ([]string, error) {
	vm, err := mem.VirtualMemory()

	if err != nil {
		return nil, err
	}

	values := []string{
		fmt.Sprintf("name:ram stat:used %d", vm.Used),
		fmt.Sprintf("name:ram stat:buffered %d", vm.Buffers),
	}

	if !s.Buffered {
		return values[:1], nil
	}
	return values, nil
}
