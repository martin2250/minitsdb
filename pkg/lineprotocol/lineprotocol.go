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
	Value float64
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

func Parse(line string) (Point, error) {
	parts := strings.Split(line, "|")

	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}

	if len(parts) < 3 {
		return Point{}, ErrInvalidFormat
	}

	// split off series and time
	var partSeries, partTime string
	{
		l := len(parts)
		partSeries, parts, partTime = parts[0], parts[1:l-1], parts[l-1]
	}

	// parse timePoint
	var timePoint int64
	if partTime != "" {
		var err error
		timePoint, err = strconv.ParseInt(partTime, 10, 64)

		if err != nil {
			return Point{}, err
		}
	} else {
		timePoint = time.Now().Unix()
	}

	// parse series tags
	tags, err := parseKVPs(strings.Fields(partSeries))

	if err != nil {
		return Point{}, err
	}

	// parse Values
	values := make([]Value, len(parts))

	for i := range parts {
		values[i], err = parseValue(parts[i])

		if err != nil {
			return Point{}, err
		}
	}

	return Point{
		Series: tags,
		Values: values,
		Time:   timePoint,
	}, nil
}

func parseValue(text string) (Value, error) {
	parts := strings.Fields(strings.TrimSpace(text))

	// need at least name and value
	if len(parts) < 2 {
		return Value{}, ErrInvalidFormat
	}

	parts, valueString := parts[:len(parts)-1], parts[len(parts)-1]

	value, err := strconv.ParseFloat(valueString, 64)

	if err != nil {
		return Value{}, err
	}

	tags, err := parseKVPs(parts)

	if err != nil {
		return Value{}, err
	}

	return Value{
		Tags:  tags,
		Value: value,
	}, nil
}

func parseKVPs(text []string) ([]KVP, error) {
	kvps := make([]KVP, len(text))
	var err error

	for i := range text {
		kvps[i], err = parseKVP(text[i])

		if err != nil {
			return nil, err
		}
	}
	return kvps, nil
}

func parseKVP(text string) (KVP, error) {
	i := strings.IndexRune(text, ':')

	// return error if ':' is
	// not present
	// the first rune
	// the last rune
	if i < 1 || i == len(text)-1 {
		return KVP{}, ErrInvalidFormat
	}

	return KVP{
		Key:   text[:i],
		Value: text[i+1:],
	}, nil
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
