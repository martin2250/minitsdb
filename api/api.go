package api

import (
	"github.com/martin2250/minitsdb/database/series/query"
	"github.com/martin2250/minitsdb/database/series/storage"
	"io"
	"log"
	"net/http"
	"strconv"
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

type HTTPQuery struct {
	Query *query.Query
	Data  chan storage.PointBuffer
}

func NewDatabaseAPI(db *database.Database) DatabaseAPI {
	return DatabaseAPI{
		queries:  fifo.NewQueue(),
		database: db,
	}
}

//AddQuery enqueues a query to be executed on the next query cycle
func (api DatabaseAPI) AddQuery(q HTTPQuery) {
	api.queries.Add(q)
}

//GetQuery returns the oldest query from the query buffer
//returns false if no query is available
func (api DatabaseAPI) GetQuery() (HTTPQuery, bool) {
	queryi := api.queries.Next()

	if queryi == nil {
		return HTTPQuery{}, false
	}

	q, ok := queryi.(HTTPQuery)

	if !ok {
		return HTTPQuery{}, false
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
	p, err := ParseQuery(request.Body)

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
				cols := i.GetIndices(colspec)
				for _, index := range cols {
					psers.Columns = append(psers.Columns, query.Column{
						Index:       index,
						Downsampler: query.DownsamplerMean,
					})
				}
			}

			query := i.Query(psers)

			httpQuery := HTTPQuery{
				Query: query,
				Data:  make(chan storage.PointBuffer, 16),
			}

			api.AddQuery(httpQuery)

			timeout := time.After(5 * 60 * time.Second)

		WriteLoop:
			for {
				select {
				case <-timeout:
					writer.Write([]byte("timeout, motherfucker!\n"))
					break WriteLoop
				case buffer, ok := <-httpQuery.Data:
					{
						if !ok {
							break WriteLoop
						}
						for i := range buffer.Time {
							io.WriteString(writer, strconv.FormatInt(buffer.Time[i], 10))
							for j := range query.Param.Columns {
								writer.Write([]byte{0x20}) // spaaaaaacee!
								io.WriteString(writer, strconv.FormatInt(buffer.Values[j][i], 10))
							}
							writer.Write([]byte{0x0A}) // newline
						}
					}
				}
			}

			writer.Write([]byte("END"))
		}
		return
	} else if strings.HasPrefix(request.URL.Path, "/query/binary") {
		http.Error(writer, "binary query api not implemented", http.StatusNotImplemented)
		return
	}

	http.Error(writer, "invalid path", http.StatusNotFound)
}
