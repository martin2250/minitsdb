package pointsource

import (
	ingest2 "github.com/martin2250/minitsdb/cmd/minitsdb-server/ingest"
	"github.com/martin2250/minitsdb/minitsdb/lineprotocol"
	"math/rand"
	"time"
)

// RandomPointSource provides random points
type RandomPointSource struct {
	Tags      map[string]string
	ValueTags []map[string]string
}

// GetPoint returns a random point as described by the options
func (rs RandomPointSource) GetPoint() (lineprotocol.Point, bool) {
	if rand.Int31n(5) == 0 {
		return lineprotocol.Point{}, false
	}

	p := lineprotocol.Point{
		Tags:   rs.Tags,
		Values: make([]ingest2.Value, len(rs.ValueTags)),
		Time:   time.Now().Unix(),
	}

	for i, tags := range rs.ValueTags {
		p.Values[i].Tags = tags
		p.Values[i].Value = 500 * (rand.Float64() - 0.5)
	}

	return p, true
}
