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

func (s *CPU) Init() error {
	var err error
	s.timesLast, err = cpu.Times(s.PerCPU)
	return err
}

func (s *CPU) Read() ([]string, error) {
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

	values := make([]string, count)
	j := 0

	for i := range timesNow {
		timeUser, timeSystem, timeBusy, timeTotal := diffTimes(timesLast[i], timesNow[i])

		if timeTotal <= 0 {
			return nil, errors.New("total time is <= zero")
		}

		values[j] = fmt.Sprintf("name:load cpu:%s stat:busy %0.3f", timesNow[i].CPU, timeBusy/timeTotal)
		j++

		if s.SystemUser {
			values[j] = fmt.Sprintf("name:load cpu:%s stat:user %0.3f", timesNow[i].CPU, timeUser/timeTotal)
			values[j+1] = fmt.Sprintf("name:load cpu:%s stat:system %0.3f", timesNow[i].CPU, timeSystem/timeTotal)
			j += 2
		}
	}

	return values, nil
}
