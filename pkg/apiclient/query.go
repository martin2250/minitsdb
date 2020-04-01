package apiclient

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"time"
)

type Column struct {
	Tags     map[string]string
	Factor   *float64 `yaml:",omitempty"`
	Function string   `yaml:",omitempty"`
}

type Query struct {
	Series    map[string]string
	Columns   []Column
	TimeStart time.Time
	TimeEnd   time.Time
	TimeStep  time.Duration
}

type queryYaml struct {
	Series    map[string]string
	Columns   []Column
	TimeStart int64
	TimeEnd   int64
	TimeStep  string
	Text      bool
}

func (q Query) Build() ([]byte, error) {
	y := queryYaml{
		Series:    q.Series,
		Columns:   q.Columns,
		TimeStart: q.TimeStart.Unix(),
		TimeEnd:   q.TimeEnd.Unix(),
		TimeStep:  fmt.Sprintf("%ds", q.TimeStep/time.Second),
		Text:      false,
	}
	return yaml.Marshal(&y)
}
