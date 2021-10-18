package param

import (
	taosTypes "github.com/taosdata/driver-go/v2/types"
	"time"
)

type Param struct {
	size   int
	value  []interface{}
	column int
}

func NewParam(size int) *Param {
	return &Param{
		size:  size,
		value: make([]interface{}, size),
	}
}

func (p *Param) SetBool(column int, value bool) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosBool(value)
}

func (p *Param) SetNull(column int) {
	if column >= p.size {
		return
	}
	p.value[column] = nil
	return
}

func (p *Param) SetTinyint(column int, value int) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosTinyint(value)
}

func (p *Param) SetSmallint(column int, value int) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosSmallint(value)
}

func (p *Param) SetInt(column int, value int) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosInt(value)
}

func (p *Param) SetBigint(column int, value int) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosBigint(value)
}

func (p *Param) SetUTinyint(column int, value uint) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosUTinyint(value)
}

func (p *Param) SetUSmallint(column int, value uint) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosUSmallint(value)
}

func (p *Param) SetUInt(column int, value uint) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosUInt(value)
}

func (p *Param) SetUBigint(column int, value uint) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosUBigint(value)
}

func (p *Param) SetFloat(column int, value float32) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosFloat(value)
}

func (p *Param) SetDouble(column int, value float64) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosDouble(value)
}

func (p *Param) SetBinary(column int, value []byte) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosBinary(value)
}

func (p *Param) SetNchar(column int, value string) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosNchar(value)
}

func (p *Param) SetTimestamp(column int, value time.Time, precision int) {
	if column >= p.size {
		return
	}
	p.value[column] = taosTypes.TaosTimestamp{
		T:         value,
		Precision: precision,
	}
}

func (p *Param) AddBool(value bool) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosBool(value)
	p.column += 1
	return p
}

func (p *Param) AddNull() *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = nil
	p.column += 1
	return p
}

func (p *Param) AddTinyint(value int) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosTinyint(value)
	p.column += 1
	return p
}

func (p *Param) AddSmallint(value int) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosSmallint(value)
	p.column += 1
	return p
}

func (p *Param) AddInt(value int) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosInt(value)
	p.column += 1
	return p
}

func (p *Param) AddBigint(value int) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosBigint(value)
	p.column += 1
	return p
}

func (p *Param) AddUTinyint(value uint) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosUTinyint(value)
	p.column += 1
	return p
}

func (p *Param) AddUSmallint(value uint) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosUSmallint(value)
	p.column += 1
	return p
}

func (p *Param) AddUInt(value uint) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosUInt(value)
	p.column += 1
	return p
}

func (p *Param) AddUBigint(value uint) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosUBigint(value)
	p.column += 1
	return p
}

func (p *Param) AddFloat(value float32) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosFloat(value)
	p.column += 1
	return p
}

func (p *Param) AddDouble(value float64) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosDouble(value)
	p.column += 1
	return p
}

func (p *Param) AddBinary(value []byte) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosBinary(value)
	p.column += 1
	return p
}

func (p *Param) AddNchar(value string) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosNchar(value)
	p.column += 1
	return p
}

func (p *Param) AddTimestamp(value time.Time, precision int) *Param {
	if p.column >= p.size {
		return p
	}
	p.value[p.column] = taosTypes.TaosTimestamp{
		T:         value,
		Precision: precision,
	}
	p.column += 1
	return p
}

func (p *Param) GetValues() []interface{} {
	return p.value
}
