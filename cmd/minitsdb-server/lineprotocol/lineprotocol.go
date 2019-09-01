package lineprotocol

import (
	"errors"
	"github.com/martin2250/minitsdb/minitsdb"
	"math"
	"strconv"
	"strings"
	"time"
)

const (
	prefixSeries  = "SERIES "
	prefixColumns = "COLUMNS "
	prefixPoint   = "POINT "
	prefixReset   = "RESET"
)

var (
	errSeriesUnset   = errors.New("no series specified")
	errColumnsUnset  = errors.New("no column map specified")
	errUnknownPrefix = errors.New("line has unknown prefix")
	errFormat        = errors.New("invalid format")
)

type Parser struct {
	db        *minitsdb.Database
	series    *minitsdb.Series
	columnMap map[int]int // maps from series column to input column
	sink      chan<- Point
}

func NewParser(db *minitsdb.Database, sink chan<- Point) Parser {
	return Parser{
		db:   db,
		sink: sink,
	}
}

type kvp struct {
	Key   string
	Value string
}

type Point struct {
	Series *minitsdb.Series
	Values []int64
}

func parseKVPs(text string) ([]kvp, error) {
	tags := strings.Split(strings.TrimSpace(text), " ")

	if len(tags) < 1 {
		return nil, errors.New("no tags specified")
	}

	kvps := make([]kvp, len(tags))

	for i, t := range tags {
		parts := strings.Split(t, ":")

		if len(parts) != 2 {
			return nil, errFormat
		}

		kvps[i].Key = parts[0]
		kvps[i].Value = parts[1]
	}

	return kvps, nil
}

func matchKVPs(kvps []kvp, tags map[string]string) bool {
	for _, kvp := range kvps {
		val, ok := tags[kvp.Key]

		if !ok || val != kvp.Value {
			return false
		}
	}
	return true
}

func (p *Parser) parseSeries(line string) error {
	kvps, err := parseKVPs(line)

	if err != nil {
		return err
	}

	for i := range p.db.Series {
		if !matchKVPs(kvps, p.db.Series[i].Tags) {
			continue
		}
		p.series = &p.db.Series[i]
		p.columnMap = nil
		return nil
	}

	return errors.New("no matching series found")
}

func (p *Parser) parseColumns(line string) error {
	if p.series == nil {
		return errSeriesUnset
	}

	colTexts := strings.Split(line, "|")

	if len(colTexts) != len(p.series.Columns) {
		return errors.New("number of columns does not match series")
	}

	colMap := make(map[int]int, len(colTexts))

	for i, colText := range colTexts {
		kvps, err := parseKVPs(colText)

		if err != nil {
			return err
		}

		for j, col := range p.series.Columns {
			if !matchKVPs(kvps, col.Tags) {
				continue
			}
			if _, ok := colMap[j]; ok {
				return errors.New("two inputs match the same column")
			}
			colMap[j] = i
			// continue to check for ambiguity
		}
	}

	if len(colMap) != len(p.series.Columns) {
		return errors.New("not all columns could be matched")
	}

	p.columnMap = colMap
	return nil
}

func (p *Parser) parsePoint(line string) error {
	switch {
	case p.series == nil:
		return errSeriesUnset
	case p.columnMap == nil:
		return errColumnsUnset
	}

	line = strings.TrimSpace(line)

	// find time
	pointTime := time.Now().Unix()

	switch parts := strings.Split(line, "|"); len(parts) {
	case 1:
		break
	case 2:
		var err error
		pointTime, err = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		if err != nil {
			return err
		}
		line = strings.TrimSpace(parts[0])
	default:
		return errFormat
	}

	// parse values
	valueTexts := strings.Split(line, " ")

	if len(valueTexts) != len(p.columnMap) {
		return errors.New("number of values does not match series")
	}

	values := make([]int64, len(valueTexts)+1)
	values[0] = pointTime

	for i, j := range p.columnMap {
		valf, err := strconv.ParseFloat(valueTexts[j], 64)

		if err != nil {
			return err
		}

		valf *= math.Pow10(p.series.Columns[i].Decimals)

		values[p.series.Columns[i].IndexPrimary] = int64(math.Round(valf))
	}

	p.sink <- Point{
		Series: p.series,
		Values: values,
	}

	return nil
}

func (p *Parser) Reset() {
	p.series = nil
	p.columnMap = nil
}

func (p *Parser) ParseLine(line string) error {
	switch {
	case strings.HasPrefix(line, prefixSeries):
		return p.parseSeries(line[len(prefixSeries):])

	case strings.HasPrefix(line, prefixColumns):
		return p.parseColumns(line[len(prefixColumns):])

	case strings.HasPrefix(line, prefixPoint):
		return p.parsePoint(line[len(prefixPoint):])

	case strings.HasPrefix(line, prefixReset):
		p.Reset()
		return nil

	default:
		return errUnknownPrefix
	}
}
