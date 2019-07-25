package ingest

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// ErrPointFormat ...
var ErrPointFormat = errors.New("point has wrong format")

// parseMap parses a key value list
// format: key1=val1,key2=val2
func parseMap(s string) (m map[string]string, err error) {
	parts := strings.Split(s, ",")
	m = make(map[string]string, len(parts))

	for _, part := range parts {
		kvpparts := strings.Split(part, "=")

		if len(kvpparts) != 2 {
			return nil, ErrPointFormat
		}

		m[kvpparts[0]] = kvpparts[1]
	}

	return
}

// ParsePoint parses a point
// todo: fix this
func ParsePoint(s string) (p Point, err error) {
	// split string
	parts := strings.Split(s, " ")

	if len(parts) == 2 {
		p.Time = time.Now().Unix()
	} else {
		if len(parts) != 3 {
			return p, ErrPointFormat
		}

		// parse time
		p.Time, err = strconv.ParseInt(parts[2], 10, 64)
		if err != nil {
			return
		}
	}

	// parse tags
	p.Tags, err = parseMap(parts[0])
	if err != nil {
		return
	}

	// parse values
	for _, pv := range strings.Split(parts[1], ";") {
		pvp := strings.Split(pv, "]=")
		if len(pvp) != 2 {
			return p, ErrPointFormat
		}

		v := Value{}

		v.Value, err = strconv.ParseFloat(pvp[1], 64)
		if err != nil {
			return
		}

		v.Tags, err = parseMap(pvp[0][1:])
		if err != nil {
			return
		}

		p.Values = append(p.Values, v)
	}

	return
}
