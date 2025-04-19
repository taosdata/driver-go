package stmt

import (
	"github.com/taosdata/driver-go/v3/common/stmt/memory"
)

type StringBuilder struct {
	builder
	rawData     []string
	everyLength []int32
	bufferLen   int
}

func NewStringBuilder(mem memory.Allocator) *StringBuilder {
	b := &StringBuilder{builder: builder{mem: mem}}
	b.refCount.Add(1)
	return b
}

func (b *StringBuilder) Release() {
	if b.refCount.Add(-1) == 0 {
		if b.nullBytes != nil {
			b.nullBytes.Release()
			b.nullBytes = nil
		}
	}
}

func (b *StringBuilder) Append(v string) {
	b.Reserve(1)
	b.UnsafeAppend(v)
}

func (b *StringBuilder) AppendNull() {
	b.Reserve(1)
	b.nullBytes.Bytes()[b.length] = 1
	b.length += 1
	b.nullLen += 1
}

func (b *StringBuilder) AppendNulls(n int) {
	for i := 0; i < n; i++ {
		b.AppendNull()
	}
}

func (b *StringBuilder) AppendEmptyValue() {
	b.Append("")
}

func (b *StringBuilder) AppendEmptyValues(n int) {
	for i := 0; i < n; i++ {
		b.AppendEmptyValue()
	}
}

func (b *StringBuilder) UnsafeAppend(v string) {
	b.nullBytes.Bytes()[b.length] = 0
	b.rawData[b.length] = v
	b.everyLength[b.length] = int32(len(v))
	b.length++
}

func (b *StringBuilder) AppendValues(v []string, isNull []byte) {
	if len(v) != len(isNull) && len(isNull) != 0 {
		panic("len(v) != len(isNull) && len(isNull) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	for i := 0; i < len(v); i++ {
		b.bufferLen += len(v[i])
		b.everyLength[b.length] = int32(len(v[i]))
	}
	copy(b.rawData[b.length:], v)

	b.builder.unsafeAppendNull(isNull, len(v))
}

func (b *StringBuilder) init(capacity int) {
	b.builder.init(capacity)
	b.rawData = make([]string, capacity)
}

func (b *StringBuilder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

func (b *StringBuilder) Resize(n int) {
	nBuilder := n
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(nBuilder, b.init)
		newArr := make([]string, n)
		copy(newArr, b.rawData)
		b.rawData = newArr
	}
}

func (b *StringBuilder) Values() []string {
	if b.rawData == nil {
		return nil
	}
	return b.rawData[:b.length]
}

func (b *StringBuilder) CopyValueBytes(dst []byte) {
	if b.rawData == nil {
		return
	}
	if len(dst) != b.bufferLen {
		panic("len(dst) != b.bufferLen")
	}
	idx := 0
	data := b.rawData[:b.length]
	for i := 0; i < len(data); i++ {
		copy(dst[idx:], data[i])
		idx += len(data[i])
	}
}

func (b *StringBuilder) ValueBytesLength() int {
	return b.bufferLen
}

func (b *StringBuilder) BindBytesLength() int {
	return 17 + b.bufferLen + b.length*4
}

func (b *StringBuilder) VariableLengthType() bool {
	return true
}

func (b *StringBuilder) BufferLengths() []int32 {
	return b.everyLength[:b.length]
}
