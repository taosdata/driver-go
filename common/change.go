package common

import "time"

func TimestampConvertToTime(timestamp int64, precision int) time.Time {
	switch precision {
	case 0: // milli-second
		return time.Unix(0, timestamp*1e6)
	case 1: // micro-second
		return time.Unix(0, timestamp*1e3)
	case 2: // nano-second
		return time.Unix(0, timestamp)
	default:
		panic("unknown precision")
	}
}

func TimeToTimestamp(t time.Time, precision int) (timestamp int64) {
	switch precision {
	case 0:
		return t.UnixNano() / 1e6
	case 1:
		return t.UnixNano() / 1e3
	case 2:
		return t.UnixNano()
	default:
		panic("unknown precision")
	}
}
