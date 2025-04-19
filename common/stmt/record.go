package stmt

import (
	"fmt"
	"sync/atomic"

	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/stmt/memory"
)

type RecordBuilder struct {
	refCount int32
	mem      memory.Allocator
	schemas  []*Stmt2AllField
	fields   []Builder
}

func NewRecordBuilder(mem memory.Allocator, schemas []*Stmt2AllField) *RecordBuilder {
	b := &RecordBuilder{
		mem:     mem,
		schemas: schemas,
		fields:  make([]Builder, len(schemas)),
	}
	atomic.AddInt32(&b.refCount, 1)

	for i := 0; i < len(schemas); i++ {
		b.fields[i] = NewBuilder(b.mem, schemas[i])
	}

	return b
}

func NewBuilder(mem memory.Allocator, schema *Stmt2AllField) Builder {
	switch schema.FieldType {
	case common.TSDB_DATA_TYPE_TINYINT:
		return NewInt8Builder(mem)
	case common.TSDB_DATA_TYPE_INT:
		return NewInt32Builder(mem)
	}
	panic(fmt.Errorf("unsupported builder for field type %d", schema.FieldType))
}

func (b *RecordBuilder) Fields() []Builder   { return b.fields }
func (b *RecordBuilder) Field(i int) Builder { return b.fields[i] }
func (b *RecordBuilder) Retain() {
	atomic.AddInt32(&b.refCount, 1)
}

func (b *RecordBuilder) Release() {
	if atomic.AddInt32(&b.refCount, -1) == 0 {
		for _, f := range b.fields {
			f.Release()
		}
		b.fields = nil
	}
}
