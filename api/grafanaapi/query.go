package grafanaapi

import (
	"encoding/json"
	"github.com/martin2250/minitsdb/api"
	"github.com/martin2250/minitsdb/database"
	"net/http"
)

type handleQuery struct {
	db *database.Database
}

func (h handleQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p, err := api.ParseQuery(r.Body)

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = p.Check()

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	enc := json.NewEncoder(w)

	matches := h.db.FindSeries(p.Series)

	// send information about the series that follow
	tagsets := make([]map[string]string, len(matches))
	for i, m := range matches {
		tagsets[i] = m.Tags
	}
	enc.Encode(tagsets)

	// send data for all
	//for _, series := range matches {
	//
	//}
}
