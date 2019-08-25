package queryhandler

import (
	"compress/gzip"
	"github.com/martin2250/minitsdb/minitsdb/storage"
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

	queries, err := queriesFromDescription(h.db, desc)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(queries) == 0 {
		http.Error(w, "request returned no values", http.StatusNotFound)
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(queries))

	writer := httpQueryResultWriter{
		output: w,
	}

	// todo: test how this affects CPU load and amount of transmitted data
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		gz := gzip.NewWriter(w)
		defer gz.Close()
		writer.output = gz
	}

	h.mux.Lock()

	for _, query := range queries {
		query.Done = &wg
		query.Sink = &writer

		params := QueryClusterParameters{
			Series: query.Series,
			Range: TimeRange{
				Start: desc.TimeStart,
				End:   desc.TimeEnd,
			},
			TimeStep: int64(desc.TimeStep / time.Second),
		}

		if cluster, ok := h.pendingQueries[params]; ok {
			cluster.Receivers = append(cluster.Receivers, query)
		} else {
			h.pendingQueries[params] = &QueryCluster{
				Parameters: params,
				Receivers:  []Query{query},
			}
			// todo: make this configurable
			time.AfterFunc(10*time.Millisecond, cluster.Execute)
		}
	}

	h.mux.Unlock()

}
