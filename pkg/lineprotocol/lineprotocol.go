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
	Tags  []KVP
	Value string // don't parse yet, need number of decimals from matching column
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
		sb.WriteString(p.Values[i].Value)
		sb.WriteByte('|')
	}

	sb.WriteString(strconv.FormatInt(p.Time, 10))

	return sb.String()
}

var ErrInvalidFormat = errors.New("invalid format")
var ErrTooLong = errors.New("input exceeds maximum length")
var ErrInvalidSym = errors.New("input has invalid symbols")

func parseKVP(text []byte, types []charType) (KVP, bool) {
	kvp := KVP{}
	indexStart := 0
	for i := range text {
		switch types[i] {
		case letter, number:
			continue
		case colon:
			break
		default:
			return KVP{}, false
		}
		// found colon
		if indexStart != 0 {
			// found second colon
			return KVP{}, false
		}
		if i == 0 {
			return KVP{}, false
		}
		indexStart = i + 1
		kvp.Key = string(text[:i])
	}
	if indexStart == 0 || indexStart == len(text) {
		return KVP{}, false
	}
	kvp.Value = string(text[indexStart:])
	return kvp, true
}

func parseValue(text []byte, types []charType) (Value, bool) {
	v := Value{}
	indexStart := 0
	numeric := true
	for i, t := range types {
		switch t {
		case colon, letter:
			numeric = false
			continue
		case number:
			continue
		case space:
			break
		default:
			return Value{}, false
		}
		//if indexStart == i {
		//	indexStart = i + 1
		//	continue // allow multiple spaces
		//}
		if numeric {
			return Value{}, false
		}
		kvp, ok := parseKVP(text[indexStart:i], types[indexStart:i])
		if !ok {
			return Value{}, false
		}
		v.Tags = append(v.Tags, kvp)
		numeric = true
		indexStart = i + 1
	}
	if v.Tags == nil || !numeric || indexStart == len(text) {
		return Value{}, false
	}
	v.Value = string(text[indexStart:])
	return v, true
}

func parseKVPs(text []byte, types []charType) ([]KVP, bool) {
	var kvps []KVP
	indexStart := 0
	for i, t := range types {
		switch t {
		case colon, letter, number:
			continue
		case space:
			break
		default:
			return nil, false
		}
		//if indexStart == i {
		//	indexStart = i + 1
		//	continue // allow multiple spaces
		//}
		kvp, ok := parseKVP(text[indexStart:i], types[indexStart:i])
		if !ok {
			return nil, false
		}
		kvps = append(kvps, kvp)
		indexStart = i + 1
	}
	if indexStart != len(types) {
		kvp, ok := parseKVP(text[indexStart:], types[indexStart:])
		if !ok {
			return nil, false
		}
		kvps = append(kvps, kvp)
	}
	return kvps, kvps != nil
}

func Parse(line []byte) (Point, error) {
	// maximum length
	if len(line) > 8192 {
		return Point{}, ErrTooLong
	}

	//line = bytes.TrimSpace(line)

	// minimum useful length
	if len(line) < 10 {
		return Point{}, ErrInvalidFormat
	}

	types := make([]charType, len(line))

	// check for characters that aren't allowed
	if !CheckSymbols(line, types) {
		return Point{}, ErrInvalidSym
	}

	p := Point{}
	indexStart := 0
	numeric := true
	hasTime := false
	for i, t := range types {
		switch t {
		case colon, letter, space:
			numeric = false
			continue
		case number:
			continue
		case pipe:
			break
		default:
			return Point{}, ErrInvalidFormat
		}
		if i == len(types)-1 {
			p.Time = time.Now().Unix()
			hasTime = true
			break
		}
		if numeric {
			return Point{}, ErrInvalidFormat
		}
		if indexStart == 0 {
			var ok bool
			p.Series, ok = parseKVPs(line[:i], types[:i])
			if !ok {
				return Point{}, ErrInvalidFormat
			}
		} else {
			v, ok := parseValue(line[indexStart:i], types[indexStart:i])
			if !ok {
				return Point{}, ErrInvalidFormat
			}
			p.Values = append(p.Values, v)
		}
		numeric = true
		indexStart = i + 1
	}
	if p.Values == nil {
		return Point{}, ErrInvalidFormat
	}
	if !numeric {
		return Point{}, ErrInvalidFormat
	}
	if !hasTime {
		var err error
		p.Time, err = strconv.ParseInt(string(line[indexStart:]), 10, 64)
		if err != nil {
			return Point{}, err
		}
	}
	return p, nil
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
func CheckSymbols(line []byte, types []charType) bool {
	l := len(line)
	for i := 0; i < l; i++ {
		t := checkChar(line[i])
		types[i] = t
		if t == other {
			return false
		}
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
