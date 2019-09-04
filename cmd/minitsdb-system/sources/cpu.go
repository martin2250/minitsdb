package sources

import (
	"errors"
	"fmt"
	"github.com/shirou/gopsutil/cpu"
)

type CPU struct {
	PerCPU     bool
	SystemUser bool

	timesLast []cpu.TimesStat
}

func getTimes(t cpu.TimesStat) (user, system, busy, total float64) {
	user = t.User
	system = t.System
	busy = t.User + t.System + t.Nice + t.Iowait + t.Irq + t.Softirq + t.Steal + t.Guest + t.GuestNice
	total = busy + t.Idle
	return
}

func diffTimes(t1, t2 cpu.TimesStat) (user, system, busy, total float64) {
	u1, s1, b1, to1 := getTimes(t1)
	u2, s2, b2, to2 := getTimes(t2)
	return u2 - u1, s2 - s1, b2 - b1, to2 - to1
}

func (s *CPU) Variables() ([]string, error) {
	var err error
	s.timesLast, err = cpu.Times(s.PerCPU)

	if err != nil {
		return nil, err
	}

	count := len(s.timesLast)
	if s.SystemUser {
		count *= 3
	}

	cols := make([]string, count)
	j := 0

	for _, t := range s.timesLast {
		cols[j] = fmt.Sprintf("name:load cpu:%s stat:%s", t.CPU, "busy")
		j++
		if s.SystemUser {
			cols[j] = fmt.Sprintf("name:load cpu:%s stat:%s", t.CPU, "user")
			cols[j+1] = fmt.Sprintf("name:load cpu:%s stat:%s", t.CPU, "system")
			j += 2
		}
	}
	return cols, nil
}

func (s *CPU) Read() ([]float64, error) {
	timesNow, err := cpu.Times(s.PerCPU)

	if err != nil {
		return nil, err
	}

	var timesLast []cpu.TimesStat
	timesLast, s.timesLast = s.timesLast, timesNow

	if timesLast == nil {
		return nil, ErrNotReady
	}

	count := len(timesLast)
	if s.SystemUser {
		count *= 3
	}

	values := make([]float64, count)
	j := 0

	for i := range timesNow {
		timeUser, timeSystem, timeBusy, timeTotal := diffTimes(timesLast[i], timesNow[i])

		if timeTotal <= 0 {
			return nil, errors.New("total time is <= zero")
		}

		values[j] = timeBusy / timeTotal
		j++

		if s.SystemUser {
			values[j] = timeUser / timeTotal
			values[j+1] = timeSystem / timeTotal
			j += 2
		}
	}

	return values, nil
}
