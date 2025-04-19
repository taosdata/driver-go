package stmt

import (
	"sync/atomic"

	"github.com/taosdata/driver-go/v3/common/stmt/bitutil"
	"github.com/taosdata/driver-go/v3/common/stmt/memory"
)

type Builder interface {
	Len() int
	Cap() int
	Retain()
	Release()
	AppendNull()
	AppendNulls(n int)
	AppendEmptyValue()
	AppendEmptyValues(n int)
	ValueBytesLength() int
	BindBytesLength() int
	CopyValueBytes(dst []byte)
	CopyNullBytes(dst []byte)
	VariableLengthType() bool
	BufferLengths() []int32
	NullBytes() []byte
}

type builder struct {
	refCount  atomic.Int64
	mem       memory.Allocator
	nullBytes *memory.Buffer
	nullLen   int
	length    int
	capacity  int
}

func (b *builder) Len() int {
	return b.length
}

func (b *builder) Cap() int {
	return b.capacity
}

func (b *builder) Retain() {
	b.refCount.Add(1)
}

func (b *builder) reserve(elements int, resize func(int)) {
	if b.length+elements > b.capacity {
		newCap := bitutil.NextPowerOf2(b.length + elements)
		resize(newCap)
	}
	if b.nullBytes == nil {
		b.nullBytes = memory.NewResizableBuffer(b.mem)
	}
}

func (b *builder) init(capacity int) {
	b.nullBytes = memory.NewResizableBuffer(b.mem)
	b.nullBytes.Resize(capacity)
	b.capacity = capacity
	memory.Set(b.nullBytes.Buf(), 0)
}

func (b *builder) unsafeAppendNull(isNull []byte, length int) {
	newLength := b.length + length
	if len(isNull) == 0 {
		memory.Set(b.nullBytes.Buf()[b.length:newLength], 0)
		b.length = newLength
		return
	}
	copy(b.nullBytes.Buf()[b.length:newLength], isNull)
	for i := 0; i < len(isNull); i++ {
		if isNull[i] == 1 {
			b.nullLen++
		}
	}
	b.length = newLength
}

func (b *builder) resize(newBytes int, init func(int)) {
	if b.nullBytes == nil {
		init(newBytes)
		return
	}
	b.nullBytes.Resize(newBytes)
	b.capacity = newBytes
	if newBytes < b.length {
		b.length = newBytes
	}
}

func (b *builder) CopyNullBytes(dst []byte) {
	if b.length == 0 {
		return
	}
	if len(dst) != b.length {
		panic("dst length is not equal to builder length")
	}
	if b.nullLen == 0 {
		return
	}
	if b.nullLen == b.length {
		memory.Set(dst, 1)
		return
	}
	copy(dst, b.nullBytes.Buf()[:b.length])
}

func (b *builder) NullBytes() []byte {
	if b.length == 0 {
		return nil
	}
	return b.nullBytes.Buf()[:b.length]
}
