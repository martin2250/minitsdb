package main

import (
	"container/list"
	"strings"
	"sync"
	"time"
)

type Error struct {
	Text string
	Time time.Time
}

type IngestBuffer struct {
	Points *list.List
	Errors *list.List
	Mux    sync.Mutex
}

func (b *IngestBuffer) CleanErrors() {
	for range time.NewTicker(10 * time.Second).C {
		now := time.Now()
		b.Mux.Lock()
		rem := b.Errors.Len() - 1000
		if rem > 0 {
			for e := b.Errors.Front(); e != nil && rem > 0; e = e.Next() {
				b.Errors.Remove(e)
				rem--
			}
		}
		for e := b.Errors.Front(); e != nil; e = e.Next() {
			err := e.Value.(Error)
			if now.Sub(err.Time) > 1*time.Hour {
				b.Errors.Remove(e)
			}
		}
		b.Mux.Unlock()
	}
}

func (b *IngestBuffer) AddError(text string) {
	b.Errors.PushBack(Error{
		Text: strings.TrimSpace(text),
		Time: time.Now(),
	})
}
