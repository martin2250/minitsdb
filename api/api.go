package api

import (
	"fmt"
	"github.com/martin2250/minitsdb/database/query"
	"github.com/martin2250/minitsdb/database/series"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/martin2250/minitsdb/database"

	fifo "github.com/foize/go.fifo"
)

// DatabaseAPI holds information on the
type DatabaseAPI struct {
	queries  *fifo.Queue
	database *database.Database
}

func NewDatabaseAPI(db *database.Database) DatabaseAPI {
	return DatabaseAPI{
		queries:  fifo.NewQueue(),
		database: db,
	}
}

//AddQuery enqueues a query to be executed on the next query cycle
func (api DatabaseAPI) AddQuery(q query.Query) {
	api.queries.Add(q)
}

//GetQuery returns the oldest query from the query buffer
//returns false if no query is available
func (api DatabaseAPI) GetQuery() (query.Query, bool) {
	queryi := api.queries.Next()

	if queryi == nil {
		return query.Query{}, false
	}

	q, ok := queryi.(query.Query)

	if !ok {
		return query.Query{}, false
	}

	return q, true
}

// ServeHTTP handles http requests to the query API, must be registered under /query
func (api DatabaseAPI) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	// check request method
	if request.Method != "POST" {
		log.Printf("invalid method: %s\n", request.Method)
		return
	}

	// parse query
	p, err := parseQuery(request.Body)

	if err != nil {
		http.Error(writer, "can't parse query", http.StatusBadRequest)
		log.Println(err)
		return
	}

	// create query parameters
	param := query.Parameters{
		TimeStep:  p.Resolution,
		Columns:   make([]query.Column, 0),
		TimeStart: p.TimeFrom,
		TimeEnd:   p.TimeTo,
	}

	// find matching series
	matches := api.database.FindSeries(p.Series)

	if len(matches) < 1 {
		http.Error(writer, "no series match query", http.StatusNotFound)
		return
	}

	// create query objects
	if strings.HasPrefix(request.URL.Path, "/query/text") {
		for _, i := range matches {
			psers := param

			for _, colspec := range p.Columns {
				cols := api.database.Series[i].GetIndices(colspec)
				for _, index := range cols {
					psers.Columns = append(psers.Columns, query.Column{
						Index:       index,
						Downsampler: series.DownsamplerMean,
					})
				}
			}

			query := query.NewQuery(&api.database.Series[i], psers, writer)

			query.Done = make(chan struct{})

			api.queries.Add(query)

			writer.Write([]byte("test\n"))

			select {
			case <-time.After(60 * time.Second):
				writer.Write([]byte("timeout, motherfucker!\n"))
			case <-query.Done:
				fmt.Fprint(os.Stderr, "done\n")
			}
		}
		return
	} else if strings.HasPrefix(request.URL.Path, "/query/binary") {
		http.Error(writer, "binary query api not implemented", http.StatusNotImplemented)
		return
	}

	http.Error(writer, "invalid path", http.StatusNotFound)
}
