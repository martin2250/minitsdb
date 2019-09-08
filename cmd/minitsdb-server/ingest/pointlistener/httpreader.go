package pointlistener

import (
	"bufio"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net/http"
	"time"
)

// returns true if the server reported that there are more points to read
func ingestRequest(sink chan<- lineprotocol.Point, address string, client *http.Client) bool {
	defer func() {
		// recover from bufio.Scanner.Scan overflows
		recover()
	}()
	req, err := http.NewRequest("GET", address, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	n := 0
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		p, err := lineprotocol.Parse(scanner.Text())
		if err != nil {
			continue
		}
		sink <- p
		n++
	}
	return n > 0 && resp.Header.Get("more") == "true"
}

func ReadIngestServer(sink chan<- lineprotocol.Point, addresses []string, shutdown chan struct{}) {
	client := http.Client{
		Timeout: 500 * time.Millisecond,
	}
	ticker := time.NewTicker(time.Second)
loop:
	for {
		select {
		case <-ticker.C:
			for _, addr := range addresses {
				for ingestRequest(sink, addr, &client) {
				}
			}
		case <-shutdown:
			close(sink)
			break loop
		}
	}
}
