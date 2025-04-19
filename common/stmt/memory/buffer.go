package memory

import "sync/atomic"

type Buffer struct {
	refCount atomic.Int64
	buf      []byte
	length   int
	mutable  bool
	mem      Allocator
}

func (b *Buffer) Release() {
	if b.mem != nil {
		if b.refCount.Add(-1) == 0 {
			if b.mem != nil {
				b.mem.Free(b.buf)
			}
			b.buf, b.length = nil, 0
		}
	}
}

func NewResizableBuffer(mem Allocator) *Buffer {
	b := &Buffer{mutable: true, mem: mem}
	b.refCount.Add(1)
	return b
}

func (b *Buffer) Bytes() []byte { return b.buf[:b.length] }

func (b *Buffer) Resize(newSize int) {
	b.resize(newSize, true)
}

func (b *Buffer) resize(newSize int, shrink bool) {
	if !shrink || newSize > b.length {
		b.Reserve(newSize)
	} else {
		// Buffer is not growing, so shrink to the requested size without
		// excess space.
		newCap := roundUpToMultipleOf64(newSize)
		if len(b.buf) != newCap {
			if newSize == 0 {
				b.mem.Free(b.buf)
				b.buf = nil
			} else {
				b.buf = b.mem.Reallocate(newCap, b.buf)
			}
		}
	}
	b.length = newSize
}

func (b *Buffer) Reserve(capacity int) {
	if capacity > len(b.buf) {
		newCap := roundUpToMultipleOf64(capacity)
		if len(b.buf) == 0 {
			b.buf = b.mem.Allocate(newCap)
		} else {
			b.buf = b.mem.Reallocate(newCap, b.buf)
		}
	}
}

func (b *Buffer) Buf() []byte { return b.buf }
