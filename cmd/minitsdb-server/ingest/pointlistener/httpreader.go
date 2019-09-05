package pointlistener

import (
	"bufio"
	"errors"
	"github.com/martin2250/minitsdb/pkg/lineprotocol"
	"net/http"
	"time"
)

func ReadIngestServer(sink chan<- lineprotocol.Point, address string) error {
	client := http.Client{
		Timeout: 500 * time.Millisecond,
	}
	for range time.NewTicker(time.Second).C {
		req, err := http.NewRequest("GET", address, nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			p, err := lineprotocol.Parse(scanner.Text())
			if err != nil {
				continue
			}
			sink <- p
		}
	}
	return errors.New("loop exited")
}
