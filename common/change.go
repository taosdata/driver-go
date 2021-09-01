package common

import "time"

func TimestampConvertToTime(timestamp int64, precision int) time.Time {
	switch precision {
	case PrecisionMilliSecond: // milli-second
		return time.Unix(0, timestamp*1e6)
	case PrecisionMicroSecond: // micro-second
		return time.Unix(0, timestamp*1e3)
	case PrecisionNanoSecond: // nano-second
		return time.Unix(0, timestamp)
	default:
		panic("unknown precision")
	}
}

func TimeToTimestamp(t time.Time, precision int) (timestamp int64) {
	switch precision {
	case PrecisionMilliSecond:
		return t.UnixNano() / 1e6
	case PrecisionMicroSecond:
		return t.UnixNano() / 1e3
	case PrecisionNanoSecond:
		return t.UnixNano()
	default:
		panic("unknown precision")
	}
}
