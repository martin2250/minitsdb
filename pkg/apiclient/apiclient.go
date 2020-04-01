package apiclient

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type ApiClient struct {
	Address    string
	HttpClient *http.Client
}

func (c *ApiClient) Query(q Query) (QueryResult, error) {
	qbuf, err := q.Build()
	if err != nil {
		return QueryResult{}, err
	}
	resp, err := c.HttpClient.Post(c.Address, "text/yaml", bytes.NewReader(qbuf))
	if err != nil {
		return QueryResult{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return QueryResult{}, fmt.Errorf("API returned status code %d", resp.StatusCode)
	}
	r := bufio.NewReader(resp.Body)

	buf, err := r.ReadBytes('\n')
	if err != nil {
		return QueryResult{}, err
	}
	var series []Series
	json.Unmarshal(buf, &series)

	return QueryResult{
		Series: series,
		r:      r,
	}, nil
}
