package queryhandler

import (
	"compress/gzip"
	"encoding/json"
	. "github.com/martin2250/minitsdb/minitsdb/types"
	"net/http"
	"strings"
	"sync"
	"time"
)

func (h *queryHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	desc, err := parseQuery(r.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	subqueries, err := queriesFromDescription(h.db, desc)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(subqueries) == 0 {
		http.Error(w, "request returned no values", http.StatusNotFound)
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(subqueries))

	querySinkTemplate := httpQueryResultWriter{
		Writer: w,
		Mux:    &sync.Mutex{},
	}

	// todo: test how this affects CPU load and amount of transmitted data
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		querySinkTemplate.Writer = gz
	}

	// send information about the series which were found
	info := make([]struct {
		Tags    map[string]string
		Columns []map[string]string
	}, len(subqueries))
	for i, subQuery := range subqueries {
		info[i].Tags = subQuery.Series.Tags
		info[i].Columns = make([]map[string]string, len(subQuery.Columns))
		for j, column := range subQuery.Columns {
			info[i].Columns[j] = column.Column.Tags
		}
	}

	err = json.NewEncoder(querySinkTemplate.Writer).Encode(info)

	if err != nil {
		h.log.WithError(err).Warning("sending query info resulted in an error")
	}

	// attach all subqueries to QueryClusters
	h.mux.Lock()

	for i, subQuery := range subqueries {
		querySink := querySinkTemplate
		querySink.Index = i
		querySink.Columns = subQuery.Columns

		subQuery.Done = &wg
		subQuery.Cancel = make(chan struct{})
		subQuery.Sink = &querySink

		params := QueryClusterParameters{
			Series: subQuery.Series,
			Range: TimeRange{
				Start: desc.TimeStart,
				End:   desc.TimeEnd,
			},
			TimeStep: int64(desc.TimeStep / time.Second),
		}

		if cluster, ok := h.pendingQueries[params]; ok {
			cluster.SubQueries = append(cluster.SubQueries, subQuery)
		} else {
			cluster = &QueryCluster{
				Parameters: params,
				SubQueries: []SubQuery{subQuery},
			}
			h.pendingQueries[params] = cluster
			// todo: make this configurable
			time.AfterFunc(100*time.Millisecond, func() {
				h.mux.Lock()
				delete(h.pendingQueries, cluster.Parameters)
				h.mux.Unlock()

				err := cluster.Execute()
				if err != nil {
					h.log.WithError(err).Warning("Query returned error")
				}
			})
		}
	}

	h.mux.Unlock()

	// create channel
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()

	// wait for either all subqueries to finish or for the request to be cancelled / timeout
	select {
	case <-r.Context().Done():
		for _, subQuery := range subqueries {
			close(subQuery.Cancel)
		}
	case <-finished:
	}

	h.log.Info("completed API request")
}
