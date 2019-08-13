package grafanaapi

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/martin2250/minitsdb/database"
	"github.com/martin2250/minitsdb/database/series"
	"github.com/martin2250/minitsdb/database/series/query"
	"github.com/martin2250/minitsdb/database/series/storage"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"
)

// handleQuery is the HTTP handler for grafana queries
type handleQuery struct {
	db  *database.Database
	add ExecutorAdder

	requests    map[seriesRequestParams]*seriesRequest
	mutRequests sync.Mutex
}

func newHandleQuery(db *database.Database, add ExecutorAdder) handleQuery {
	return handleQuery{
		db:          db,
		add:         add,
		requests:    make(map[seriesRequestParams]*seriesRequest),
		mutRequests: sync.Mutex{},
	}
}

type seriesRequestParams struct {
	s *series.Series
	// query params
	timeStep  int64
	timeStart int64
	timeEnd   int64
}

// data object returned by a seriesRequest to all it's waiting http listeners via their channels
// index is the index of the series within that request that was served
// returning this with an empty data field
type seriesRequestData struct {
	data  *storage.PointBuffer
	index int
	err   error
}

// seriesRequestReceiver holds a channel for seriesRequestData and the index of the series that is serves within that request
type seriesRequestReceiver struct {
	pipe  chan<- seriesRequestData
	index int
	// counts the number of active seriesRequests for this column, must be decremented atomically before seriesRequest is discarded
	// todo: replace this with err EOF
	activeCounter *int64
}

// seriesRequest groups multiple requests for the same series, time step and range
// multiple such requests are grouped so that a dashboard load in grafana does not trigger mutliple queries to run at once
type seriesRequest struct {
	params seriesRequestParams
	// the receivers
	receivers []seriesRequestReceiver
	// columns to be read (unordered)
	columns []query.Column
}

type seriesInfo struct {
	Tags    map[string]string
	Columns []map[string]string
}

func (r *seriesRequest) sendToAll(d seriesRequestData) bool {
	n := 0

	for i, recv := range r.receivers {
		if recv.pipe != nil {
			d.index = recv.index
			select {
			case recv.pipe <- d:
				n += 1
			case <-time.After(100 * time.Millisecond):
				r.receivers[i].pipe = nil
				log.WithFields(log.Fields{
					"ptr": fmt.Sprintf("%p", r),
				}).Error("receiver timeout")
			}
		}
	}
	return n != 0
}

func (r *seriesRequest) Execute() error {
	log.WithFields(log.Fields{
		"ptr":       fmt.Sprintf("%p", r),
		"series":    r.params.s.Tags,
		"start":     r.params.timeStart,
		"end":       r.params.timeEnd,
		"step":      r.params.timeStep,
		"receivers": len(r.receivers),
		"columns":   len(r.columns),
	}).Info("Executing series request")

	sort.Slice(r.columns, func(i, j int) bool {
		return r.columns[i].Index < r.columns[j].Index
	})

	// todo: really important: unscramble data again (aka provide a map to the receivers)

	q := r.params.s.Query(query.Parameters{
		TimeStep:  r.params.timeStep,
		Columns:   r.columns,
		TimeStart: r.params.timeStart,
		TimeEnd:   r.params.timeEnd,
	})

	for {
		buf, err := q.ReadNext()

		if err == nil && buf.Cols() != len(r.columns) {
			log.WithFields(log.Fields{
				"ptr":      fmt.Sprintf("%p", r),
				"expected": len(r.columns),
				"got":      buf.Cols(),
			}).Error("number of columns read does not match")

			return nil
		}

		if err != nil || buf.Len() > 0 {
			anyReceivers := r.sendToAll(seriesRequestData{
				data: &buf,
				err:  err,
			})

			if !anyReceivers {
				log.WithFields(log.Fields{
					"ptr": fmt.Sprintf("%p", r),
				}).Info("all receivers closed")

				return err
			}
		}

		if err != nil {
			if err == io.EOF {
				log.WithFields(log.Fields{
					"ptr": fmt.Sprintf("%p", r),
				}).Info("Finished series request")
			} else {
				log.WithFields(log.Fields{
					"ptr": fmt.Sprintf("%p", r),
				}).Warn(err.Error())
			}

			return err
		}
	}
}

func logHTTPError(w http.ResponseWriter, r *http.Request, error string, code int) {
	log.WithFields(log.Fields{
		"code":   code,
		"error":  error,
		"client": r.RemoteAddr,
		"url":    r.URL,
	}).Warning("API request failed")
	http.Error(w, error, code)
}

func (h handleQuery) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// decode query parameters
	par := struct {
		Series  map[string]string
		Columns []struct {
			Tags        map[string]string
			Downsampler string
		}
		TimeStep  time.Duration
		TimeStart int64
		TimeEnd   int64
		Wait      bool
	}{}

	err := yaml.NewDecoder(r.Body).Decode(&par)

	if err != nil {
		logHTTPError(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	log.WithFields(log.Fields{
		"client": r.RemoteAddr,
		"url":    r.URL,
		"par":    par,
	}).Info("Received API request")

	// check query parameters
	if par.TimeEnd < par.TimeStart {
		logHTTPError(w, r, "query range must start before it ends", http.StatusBadRequest)
		return
	}

	if par.TimeStep%time.Second != 0 || par.TimeStep < time.Second {
		logHTTPError(w, r, "time step must be a positive integer multiple of 1s", http.StatusBadRequest)
		return
	}

	timeStep := int64(par.TimeStep / time.Second)

	if par.Series == nil || len(par.Series) < 1 {
		logHTTPError(w, r, "series description missing", http.StatusBadRequest)
		return
	}

	if par.Columns == nil || len(par.Columns) < 1 {
		logHTTPError(w, r, "column description missing", http.StatusBadRequest)
		return
	}

	// find matching series
	matches := h.db.FindSeries(par.Series)

	if len(matches) < 1 {
		logHTTPError(w, r, "no matches", http.StatusNotFound)
		return
	}

	// construct query columns for each series
	queryColumns := make([][]query.Column, len(matches))

	for i, s := range matches { // loop over all series that match the query
		queryColumns[i] = make([]query.Column, 0, len(par.Columns))

		for _, pCol := range par.Columns { // loop over all column descriptions in the query
			if pCol.Downsampler != "" {
				logHTTPError(w, r, "unknown downsampler", http.StatusNotFound)
			}
			matches := s.GetIndices(pCol.Tags)

			for _, iCol := range matches {
				// todo: check if column supports downsampler, else skip; don't error
				// also actually implement different downsamplers
				queryColumns[i] = append(queryColumns[i], query.Column{
					Index:       iCol,
					Downsampler: query.DownsamplerMean,
				})
			}
		}
	}

	// register query for every matching series
	receiver := make(chan seriesRequestData, 16)
	columnMaps := make([][]int, len(matches))

	h.mutRequests.Lock()

	for indexMatch, columnsMatch := range queryColumns {
		// create seriesRequest, if it doesn't exists
		srp := seriesRequestParams{
			s:         matches[indexMatch],
			timeStep:  timeStep,
			timeStart: par.TimeStart,
			timeEnd:   par.TimeEnd,
		}
		request, ok := h.requests[srp]

		// if none was found, create one
		if !ok {
			request = &seriesRequest{
				params:    srp,
				receivers: make([]seriesRequestReceiver, 0, 1),
				columns:   make([]query.Column, 0, len(columnsMatch)),
			}

			if par.Wait {
				h.requests[srp] = request
				// after x time, actually let the main goroutine execute the query
				time.AfterFunc(100*time.Millisecond, func() {
					h.mutRequests.Lock()
					sr := h.requests[srp]
					delete(h.requests, srp)
					h.mutRequests.Unlock()

					h.add.Add(sr)
				})
			}
		}

		// add pipe to seriesRequest receiver
		request.receivers = append(request.receivers, seriesRequestReceiver{
			pipe:  receiver,
			index: indexMatch,
		})

		// map query columns to columns of the seriesRequest
		columnMaps[indexMatch] = make([]int, len(columnsMatch))
	LoopMatchingColumns:
		for iQuery, colQuery := range columnsMatch {
			for iReq, colReq := range request.columns {
				if colQuery.Downsampler == colReq.Downsampler && colQuery.Index == colReq.Index {
					columnMaps[indexMatch][iQuery] = iReq
					continue LoopMatchingColumns
				}
			}
			columnMaps[indexMatch][iQuery] = len(request.columns)
			request.columns = append(request.columns, colQuery)
		}

		if !ok && !par.Wait {
			h.add.Add(request)
		}
	}

	h.mutRequests.Unlock()

	// send data to client
	enc := json.NewEncoder(w)

	// send information about the series that follow
	tagsets := make([]seriesInfo, len(matches))
	for i, m := range matches {
		tagsets[i].Tags = m.Tags
		for _, col := range queryColumns[i] {
			tagsets[i].Columns = append(tagsets[i].Columns, m.Columns[col.Index].Tags)
		}
	}
	err = enc.Encode(tagsets)

	if err != nil {
		logHTTPError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	numActiveRequests := int64(len(matches))

	for numActiveRequests > 0 {
		result := <-receiver

		if result.err == io.EOF {
			numActiveRequests -= 1
			continue
		} else if result.err != nil {
			logHTTPError(w, r, result.err.Error(), http.StatusNotFound)
			return
		}

		// execute queries on different series in parallel, interleave results as they come in
		err := enc.Encode(struct {
			SeriesIndex int
			NumValues   int
			NumPoints   int
		}{
			SeriesIndex: result.index,
			NumPoints:   result.data.Len(),
			NumValues:   len(columnMaps[result.index]),
		})

		if err != nil {
			logHTTPError(w, r, err.Error(), http.StatusInternalServerError)
			return
		}

		err = binary.Write(w, binary.LittleEndian, result.data.Time)

		if err != nil {
			logHTTPError(w, r, err.Error(), http.StatusInternalServerError)
			return
		}

		for _, i := range columnMaps[result.index] {
			// todo: convert to float
			err = binary.Write(w, binary.LittleEndian, result.data.Values[i])

			if err != nil {
				logHTTPError(w, r, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	log.Info("Completed API request")
}
