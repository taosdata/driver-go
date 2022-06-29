package common

import "database/sql/driver"

const DefaultHttpPort = 6041

type TDEngineRestfulResp struct {
	Code      int
	Rows      int
	Desc      string
	ColNames  []string
	ColTypes  []int
	ColLength []int64
	Data      [][]driver.Value
}
