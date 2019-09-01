package main

import (
	"strings"
	"sync"
)

const (
	prefixSeries  = "SERIES "
	prefixColumns = "COLUMNS "
	prefixPoint   = "POINT "
	prefixReset   = "RESET"
)

type LineProtocolEmulator struct {
	Buffer *IngestBuffer
	col    *PointCollection
}

func (l *LineProtocolEmulator) Reset() {
	if l.col != nil {
		l.col.Mux.Lock()
		l.col.Active = false
		l.col.Mux.Unlock()
		l.col = nil
	}
}

func (l *LineProtocolEmulator) NewCol(series string) {
	l.Reset()
	l.Buffer.Mux.Lock()
	l.Buffer.Buffer = append(l.Buffer.Buffer, PointCollection{
		Series:  series,
		Columns: "",
		Values:  make(chan string, 10000),
		Active:  true,
		Mux:     sync.Mutex{},
	})
	l.col = &l.Buffer.Buffer[len(l.Buffer.Buffer)-1]
	l.Buffer.Mux.Unlock()
}

func (l *LineProtocolEmulator) Parse(line string) {
	switch {
	case strings.HasPrefix(line, prefixReset):
		l.Reset()
	case strings.HasPrefix(line, prefixSeries):
		l.NewCol(line)
	case strings.HasPrefix(line, prefixColumns):
		if l.col != nil {
			if l.col.Columns != "" {
				l.NewCol(l.col.Series)
			}
			l.col.Mux.Lock()
			l.col.Columns = line
			l.col.Mux.Unlock()
		} else {
			l.Reset()
		}
	case strings.HasPrefix(line, prefixPoint):
		if l.col != nil && l.col.Series != "" && l.col.Columns != "" {
			l.col.Values <- line
		}
	}
}
