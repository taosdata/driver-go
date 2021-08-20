package types

import (
	"time"
)

type TaosBool bool
type TaosTinyint int8
type TaosSmallint int16
type TaosInt int32
type TaosBigint int64
type TaosUTinyint uint8
type TaosUSmallint uint16
type TaosUInt uint32
type TaosUBigint uint64
type TaosFloat float32
type TaosDouble float64
type TaosBinary []byte
type TaosNchar string
type TaosTimestamp struct {
	T         time.Time
	Precision int
}
