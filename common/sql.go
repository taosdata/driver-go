package common

import (
	"bytes"
	"database/sql/driver"
	"errors"
	"strconv"
	"strings"
	"time"
)

func InterpolateParams(query string, args []driver.Value) (string, error) {
	// Number of ? should be same to len(args)
	if strings.Count(query, "?") != len(args) {
		return "", driver.ErrSkip
	}
	buf := bytes.NewBufferString("")
	argPos := 0

	for i := 0; i < len(query); i++ {
		q := strings.IndexByte(query[i:], '?')
		if q == -1 {
			buf.WriteString(query[i:])
			break
		}
		buf.WriteString(query[i : i+q])
		i += q

		arg := args[argPos]
		argPos++

		if arg == nil {
			buf.WriteString("NULL")
			continue
		}
		switch v := arg.(type) {
		case int64:
			buf.WriteString(strconv.FormatInt(v, 10))
		case uint64:
			buf.WriteString(strconv.FormatUint(v, 10))
		case float64:
			buf.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
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
			buf.Write(v)
		case string:
			buf.WriteString(v)
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
