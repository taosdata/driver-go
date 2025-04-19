package stmt

import (
	"unsafe"

	"github.com/taosdata/driver-go/v3/common/mem"
	"github.com/taosdata/driver-go/v3/common/stmt/memory"
)

const (
	minBuilderCapacity = 1 << 5
)

type Int8Builder struct {
	builder

	data    *memory.Buffer
	rawData []int8
}

func NewInt8Builder(mem memory.Allocator) *Int8Builder {
	b := &Int8Builder{builder: builder{mem: mem}}
	b.refCount.Add(1)
	return b
}

func (b *Int8Builder) Release() {
	if b.refCount.Add(-1) == 0 {
		if b.nullBytes != nil {
			b.nullBytes.Release()
			b.nullBytes = nil
		}
		if b.data != nil {
			b.data.Release()
			b.data = nil
			b.rawData = nil
		}
	}
}

func (b *Int8Builder) Append(v int8) {
	b.Reserve(1)
	b.UnsafeAppend(v)
}

func (b *Int8Builder) AppendNull() {
	b.Reserve(1)
	b.nullBytes.Bytes()[b.length] = 1
	b.length += 1
	b.nullLen += 1
}

func (b *Int8Builder) AppendNulls(n int) {
	for i := 0; i < n; i++ {
		b.AppendNull()
	}
}

func (b *Int8Builder) AppendEmptyValue() {
	b.Append(0)
}

func (b *Int8Builder) AppendEmptyValues(n int) {
	for i := 0; i < n; i++ {
		b.AppendEmptyValue()
	}
}

func (b *Int8Builder) UnsafeAppend(v int8) {
	b.nullBytes.Bytes()[b.length] = 0
	b.rawData[b.length] = v
	b.length += 1
}

func (b *Int8Builder) AppendValues(v []int8, isNull []byte) {
	if len(v) != len(isNull) && len(isNull) != 0 {
		panic("len(v) != len(isNull) && len(isNull) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	copy(b.rawData[b.length:], v)

	b.builder.unsafeAppendNull(isNull, len(v))
}

const Int8SizeBytes = int(unsafe.Sizeof(int8(0)))

func (b *Int8Builder) init(capacity int) {
	b.builder.init(capacity)
	b.data = memory.NewResizableBuffer(b.mem)
	bytesN := Int8SizeBytes * capacity
	b.data.Resize(bytesN)
	b.rawData = Int8CastFromBytes(b.data.Bytes())
}

func (b *Int8Builder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

func (b *Int8Builder) Resize(n int) {
	nBuilder := n
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(nBuilder, b.init)
		b.data.Resize(Int8SizeBytes * n)
		b.rawData = Int8CastFromBytes(b.data.Bytes())
	}
}

func (b *Int8Builder) Values() []int8 {
	if b.rawData == nil {
		return nil
	}
	return b.rawData[:b.length]
}

func (b *Int8Builder) CopyValueBytes(dst []byte) {
	if b.rawData == nil {
		return
	}
	valueBytesLen := b.length * Int8SizeBytes
	if len(dst) != valueBytesLen {
		panic("len(dst) != valueBytesLen")
	}
	mem.CopyUncheck(unsafe.Pointer(&b.data.Buf()[0]), unsafe.Pointer(&dst[0]), valueBytesLen)
}

func (b *Int8Builder) ValueBytesLength() int {
	return b.length * Int8SizeBytes
}

func (b *Int8Builder) BindBytesLength() int {
	return 17 + b.length*Int8SizeBytes + b.length
}

func (b *Int8Builder) VariableLengthType() bool {
	return false
}

func (b *Int8Builder) BufferLengths() []int32 {
	return nil
}

type Int32Builder struct {
	builder

	data    *memory.Buffer
	rawData []int32
}

func NewInt32Builder(mem memory.Allocator) *Int32Builder {
	b := &Int32Builder{builder: builder{mem: mem}}
	b.refCount.Add(1)
	return b
}

func (b *Int32Builder) Release() {
	if b.refCount.Add(-1) == 0 {
		if b.nullBytes != nil {
			b.nullBytes.Release()
			b.nullBytes = nil
		}
		if b.data != nil {
			b.data.Release()
			b.data = nil
			b.rawData = nil
		}
	}
}

func (b *Int32Builder) Append(v int32) {
	b.Reserve(1)
	b.UnsafeAppend(v)
}

func (b *Int32Builder) AppendNull() {
	b.Reserve(1)
	b.nullBytes.Bytes()[b.length] = 1
	b.length += 1
}

func (b *Int32Builder) AppendNulls(n int) {
	for i := 0; i < n; i++ {
		b.AppendNull()
	}
}

func (b *Int32Builder) AppendEmptyValue() {
	b.Append(0)
}

func (b *Int32Builder) AppendEmptyValues(n int) {
	for i := 0; i < n; i++ {
		b.AppendEmptyValue()
	}
}

func (b *Int32Builder) UnsafeAppend(v int32) {
	b.nullBytes.Bytes()[b.length] = 0
	b.rawData[b.length] = v
	b.length += 1
}

func (b *Int32Builder) AppendValues(v []int32, isNull []byte) {
	if len(v) != len(isNull) && len(isNull) != 0 {
		panic("len(v) != len(isNull) && len(isNull) != 0")
	}

	if len(v) == 0 {
		return
	}

	b.Reserve(len(v))
	copy(b.rawData[b.length:], v)
	b.builder.unsafeAppendNull(isNull, len(v))
}

const Int32SizeBytes = int(unsafe.Sizeof(int32(0)))

func (b *Int32Builder) init(capacity int) {
	b.builder.init(capacity)
	b.data = memory.NewResizableBuffer(b.mem)
	bytesN := Int32SizeBytes * capacity
	b.data.Resize(bytesN)
	b.rawData = Int32CastFromBytes(b.data.Bytes())
}

func (b *Int32Builder) Reserve(n int) {
	b.builder.reserve(n, b.Resize)
}

func (b *Int32Builder) Resize(n int) {
	nBuilder := n
	if n < minBuilderCapacity {
		n = minBuilderCapacity
	}

	if b.capacity == 0 {
		b.init(n)
	} else {
		b.builder.resize(nBuilder, b.init)
		b.data.Resize(Int32SizeBytes * n)
		b.rawData = Int32CastFromBytes(b.data.Bytes())
	}
}

func (b *Int32Builder) Values() []int32 {
	if b.rawData == nil {
		return nil
	}
	return b.rawData[:b.length]
}

func (b *Int32Builder) CopyValueBytes(dst []byte) {
	if b.rawData == nil {
		return
	}
	valueBytesLen := b.length * Int32SizeBytes
	if len(dst) != valueBytesLen {
		panic("len(dst) != valueBytesLen")
	}
	mem.CopyUncheck(unsafe.Pointer(&b.data.Buf()[0]), unsafe.Pointer(&dst[0]), valueBytesLen)
}

func (b *Int32Builder) ValueBytesLength() int {
	return b.length * Int32SizeBytes
}

func (b *Int32Builder) BindBytesLength() int {
	return 17 + b.length*Int32SizeBytes + b.length
}

func (b *Int32Builder) VariableLengthType() bool {
	return false
}

func (b *Int32Builder) BufferLengths() []int32 {
	return nil
}
