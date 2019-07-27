package api

import (
	"log"
	"net/http"
	"strings"

	"github.com/martin2250/minitsdb/database"

	fifo "github.com/foize/go.fifo"
	"github.com/martin2250/minitsdb/database/series"
)

// QueryExecutor gets queued by the http goroutine and is executed by the main database thread
type QueryExecutor struct {
	responseWriter http.ResponseWriter

	series []series.Series
}

// DatabaseAPI holds information on the
type DatabaseAPI struct {
	queries  *fifo.Queue
	database *database.Database
}

// AddQuery enqueues a query to be executed on the next query cycle
// func (api DatabaseAPI) AddQuery(q QueryExecutor) {
// 	api.queries.Add(q)
// }

// GetQuery returns the oldest query from the query buffer
// returns false if no query is available
// func (api DatabaseAPI) GetQuery() (QueryExecutor, bool) {
// 	queryi := api.queries.Next()

// 	if queryi == nil {
// 		return nil, false
// 	}

// 	query, ok := queryi.(QueryExecutor)

// 	if !ok {
// 		return nil, false
// 	}

// 	return query, true
// }

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
		serveError(writer, err.Error())
		log.Println(err)
		return
	}

	// find matching series
	series := api.database.FindSeries(p.Series)

	if len(series) < 1 {
		serveError(writer, "no series match query")
		return
	}

	// create query objects
	if strings.HasPrefix(request.URL.Path, "/query/text") {

	}

	if strings.HasPrefix(request.URL.Path, "/query/binary") {
		serveError(writer, "binary query api not implemented")
	}

	serveError(writer, "invalid path")
}

func serveError(writer http.ResponseWriter, err string) {
	writer.WriteHeader(400)

	writer.Write([]byte("error: " + err))
}
