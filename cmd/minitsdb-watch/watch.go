package main

import (
	"fmt"
	"github.com/martin2250/minitsdb/pkg/apiclient"
	"time"
)

type Watcher struct {
	Config WatchConfig
	Client apiclient.ApiClient
	Notify func(series map[string]string, column map[string]string, message string)
}

type CheckResult int

const (
	Ok CheckResult = iota
	ErrorRange
)

func (w *Watcher) CheckValues(times []int64, values []float64) CheckResult {
	if w.Config.Range != nil {
		r := *w.Config.Range
		var sum float64
		for _, v := range values {
			sum += v
		}
		avg := sum / float64(len(values))
		if avg > r.Max || avg < r.Min {
			return ErrorRange
		}
	}

	return Ok
}

func (w *Watcher) Check(end time.Time) {
	q := apiclient.Query{
		Series:    w.Config.Series,
		Columns:   []apiclient.Column{w.Config.Column},
		TimeStart: end.Add(-w.Config.TimeRange),
		TimeEnd:   end,
		TimeStep:  w.Config.TimeStep,
	}
	req, err := w.Client.Query(q)
	if err != nil {
		fmt.Println("error creating request", err)
	}
	chunks, err := req.ReadAll()
	if err != nil {
		fmt.Println("error reading chunks", err)
		return
	}
	for i, s := range req.Series {
		if w.Config.MinPoints != nil {
			if len(chunks[i].Times) < *w.Config.MinPoints {
				w.Notify(s.Tags, nil, "not enough points")
			}
		}
		if len(chunks[i].Times) == 0 {
			continue
		}
		for j, col := range s.Columns {
			res := w.CheckValues(chunks[i].Times, chunks[i].Values[j])
			if res == Ok {
				continue
			}
			switch res {
			case ErrorRange:
				w.Notify(s.Tags, col, "out of range")
			}
		}
	}
}

func (w *Watcher) Run() {
	ticker := time.Tick(w.Config.Interval)
	for {
		now := <-ticker
		w.Check(now)
	}
}
