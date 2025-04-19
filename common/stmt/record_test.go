package stmt

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/taosdata/driver-go/v3/common"
	"github.com/taosdata/driver-go/v3/common/stmt/memory"
)

func TestRecord(t *testing.T) {
	allocator := memory.NewGoAllocator()
	schemas := []*Stmt2AllField{
		{
			FieldType: common.TSDB_DATA_TYPE_TINYINT,
		},
	}
	rb := NewRecordBuilder(allocator, schemas)
	i8Builder := rb.Field(0).(*Int8Builder)
	i8Builder.Append(1)
	values := i8Builder.Values()
	nullList := i8Builder.NullBytes()
	t.Log(values, nullList)

	i8Builder.AppendNull()
	values = i8Builder.Values()
	nullList = i8Builder.NullBytes()
	t.Log(values, nullList)
	i8Builder.Append(2)
	values = i8Builder.Values()
	nullList = i8Builder.NullBytes()
	t.Log(values, nullList)
	i8Builder.AppendValues([]int8{12, 13, 14}, nil)
	values = i8Builder.Values()
	nullList = i8Builder.NullBytes()
	t.Log(values, nullList)
	i8Builder.AppendNulls(2)
	values = i8Builder.Values()
	nullList = i8Builder.NullBytes()
	t.Log(values, nullList)
	expect := []int8{1, 0, 2, 12, 13, 14, 0, 0}
	expectNull := []byte{0, 1, 0, 0, 0, 0, 1, 1}
	assert.Equal(t, expect, values)
	assert.Equal(t, expectNull, nullList)
}
