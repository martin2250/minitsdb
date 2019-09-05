package lineprotocol

import (
	"errors"
	"strconv"
	"strings"
	"time"
)

// Line Protcol Format:
// "S|C|C|C|" or "S|C|C|C|T"

type KVP struct {
	Key   string
	Value string
}

type Value struct {
	Tags []KVP

	Value    int64
	Decimals int
}

type Point struct {
	Series []KVP
	Values []Value
	Time   int64
}

func writeKVPs(sb *strings.Builder, kvps []KVP) {
	for i := range kvps {
		if i != 0 {
			sb.WriteByte(' ')
		}
		sb.WriteString(kvps[i].Key)
		sb.WriteByte(':')
		sb.WriteString(kvps[i].Value)
	}
}

func (p Point) String() string {
	var sb strings.Builder

	writeKVPs(&sb, p.Series)
	sb.WriteByte('|')

	for i := range p.Values {
		writeKVPs(&sb, p.Values[i].Tags)
		sb.WriteByte(' ')
		sb.WriteString(strconv.FormatFloat(p.Values[i].Value, 'g', -1, 64))
		sb.WriteByte('|')
	}

	sb.WriteString(strconv.FormatInt(p.Time, 10))

	return sb.String()
}

var ErrInvalidFormat = errors.New("invalid format")
var ErrTooLong = errors.New("input exceeds maximum length")
var ErrInvalidSym = errors.New("input has invalid symbols")

type parserState int

const (
	stateSeries parserState = iota
	stateColumn
	stateTime
	stateBetween
)

type kvState int

const (
	stateKey kvState = iota
	stateValue
	stateDone
	stateError
)

func Parse(line []byte) (Point, error) {
	// maximum length
	if len(line) > 8192 {
		return Point{}, ErrTooLong
	}

	// minimum useful length
	if len(line) < 10 {
		return Point{}, ErrInvalidFormat
	}

	// check for characters that aren't allowed
	if !CheckSymbols(line) {
		return Point{}, ErrInvalidSym
	}

	p := Point{}

	state := stateSeries
	indexStartToken := 0
	indexStartKV := 0

	kvp := KVP{}
	val := Value{}

	var key, value []byte

	for i, b := range line {
		t := checkChar(line[i])

		if t != letter && t != number {
			if key == nil {

			}
		}

		switch state {

		case stateKvKey:
			switch t {
			case space:
				indexStart = i + 1
			case letter, number:
				continue
			case colon:
				kvp.Key = string(line[indexStart:i])
				indexStart = i + 1
				state = stateKvVal
			default:
				return Point{}, ErrInvalidFormat
			}

		case stateKvVal:
			switch t {
			case letter, number:
				continue
			case space, pipe:
				kvp.Value = string(line[indexStart:i])
				state = stateBetween
				if seriesDone {
					val.Tags = append(val.Tags, kvp)
				} else {
					p.Series = append(p.Series, kvp)
				}
				if t == pipe {
					seriesDone = true
				}
			default:
				return Point{}, ErrInvalidFormat
			}

		case stateBetween:
			switch t {

			}
		}
	}
}

type charType byte

const (
	letter charType = iota
	number
	other
	pipe  charType = '|'
	colon charType = ':'
	space charType = ' '
)

func checkChar(b byte) charType {
	if (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || b == '_' {
		return letter
	}

	if (b >= '0' && b <= '9') || b == '.' || b == '-' {
		return number
	}

	if b == '|' || b == ':' || b == ' ' {
		return charType(b)
	}

	return other
}

// CheckSymbols checks if the line contains symbols other than
// a-z A-Z 0-9 . : | space -
func CheckSymbols(line []byte) bool {
	l := len(line)
	for i := 0; i < l; i++ {
		t := checkChar(line[i])

		if t != other {
			continue
		}

		return false
	}

	return true
}

func MatchKVPs(kvps []KVP, tags map[string]string) bool {
	for _, kvp := range kvps {
		val, ok := tags[kvp.Key]

		if !ok || val != kvp.Value {
			return false
		}
	}
	return true
}
