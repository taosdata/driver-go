package common

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func InterpolateParams(query string, args []driver.NamedValue) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}
	buf := &strings.Builder{}
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf.WriteString(query[i:])
			break
		}
		buf.WriteString(query[i : i+q])
		i += q

		arg := args[argPos].Value
		argPos++

		if arg == nil {
			buf.WriteString("NULL")
			continue
		}
		switch v := arg.(type) {
		case int8:
			buf.WriteString(strconv.FormatInt(int64(v), 10))
		case int16:
			buf.WriteString(strconv.FormatInt(int64(v), 10))
		case int32:
			buf.WriteString(strconv.FormatInt(int64(v), 10))
		case int64:
			buf.WriteString(strconv.FormatInt(v, 10))
		case uint8:
			buf.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint16:
			buf.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint32:
			buf.WriteString(strconv.FormatUint(uint64(v), 10))
		case uint64:
			buf.WriteString(strconv.FormatUint(v, 10))
		case float32:
			fmt.Fprintf(buf, "%f", v)
		case float64:
			fmt.Fprintf(buf, "%f", v)
		case int:
			buf.WriteString(strconv.Itoa(v))
		case uint:
			buf.WriteString(strconv.FormatUint(uint64(v), 10))
		case bool:
			if v {
				buf.WriteByte('1')
			} else {
				buf.WriteByte('0')
			}
		case time.Time:
			t := v.Format(time.RFC3339Nano)
			buf.WriteByte('\'')
			buf.WriteString(t)
			buf.WriteByte('\'')
		case []byte:
			if bytesHasBackslash(v) {
				return "", driver.ErrSkip
			}
			buf.WriteByte('\'')
			buf.Write(v)
			buf.WriteByte('\'')
		case string:
			if stringHasBackslash(v) {
				return "", driver.ErrSkip
			}
			buf.WriteByte('\'')
			buf.WriteString(v)
			buf.WriteByte('\'')
		default:
			return "", driver.ErrSkip
		}
		if buf.Len() > MaxTaosSqlLen {
			return "", errors.New("sql statement exceeds the maximum length")
		}
	}
	if argPos != len(args) {
		return "", driver.ErrSkip
	}
	return buf.String(), nil
}

func ValueArgsToNamedValueArgs(args []driver.Value) (values []driver.NamedValue) {
	values = make([]driver.NamedValue, len(args))
	for i, arg := range args {
		values[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   arg,
		}
	}
	return
}

func bytesHasBackslash(v []byte) bool {
	escapeRunes := []rune{'\'', '"', '\\', '\x00', '\n', '\r', '\x1a'}
	for _, r := range escapeRunes {
		if bytes.ContainsRune(v, r) {
			return true
		}
	}
	return false
}

func stringHasBackslash(v string) bool {
	escapeRunes := []rune{'\'', '"', '\\', '\x00', '\n', '\r', '\x1a'}
	for _, r := range escapeRunes {
		if strings.ContainsRune(v, r) {
			return true
		}
	}
	return false
}
