package api

import (
	"encoding/json"
	"github.com/martin2250/minitsdb/minitsdb"
	"io"
	"net/http"
)

type handleList struct {
	db *minitsdb.Database
}

type handleListColumn struct {
	Tags     map[string]string
	Decimals int
}

type handleListSeries struct {
	Tags     map[string]string
	TimeStep int64
	Columns  []handleListColumn
}

func (h handleList) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// decode filter (if any)
	var filter map[string]string

	if err := json.NewDecoder(r.Body).Decode(&filter); err != nil {
		if err != io.EOF {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

	// find series
	var matches []*minitsdb.Series

	if filter == nil {
		matches = make([]*minitsdb.Series, len(h.db.Series))
		for i := range h.db.Series {
			matches[i] = &h.db.Series[i]
		}
	} else {
		matches = h.db.FindSeries(filter, true)
	}

	// encode series
	data := make([]handleListSeries, len(matches))

	for i, s := range matches {
		data[i].Tags = s.Tags
		data[i].TimeStep = s.Buckets[0].TimeStep
		data[i].Columns = make([]handleListColumn, len(s.Columns))
		for j, c := range s.Columns {
			data[i].Columns[j].Tags = c.Tags
			data[i].Columns[j].Decimals = c.Decimals
		}
	}

	// send data
	enc := json.NewEncoder(w)
	enc.SetIndent("", " ")
	enc.Encode(data)
}
