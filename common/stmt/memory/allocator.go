package memory

const (
	alignment = 64
)

type Allocator interface {
	Allocate(size int) []byte
	Reallocate(size int, b []byte) []byte
	Free(b []byte)
}
