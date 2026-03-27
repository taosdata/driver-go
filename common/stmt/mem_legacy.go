//go:build !go1.17
// +build !go1.17

package stmt

import "unsafe"

type slice struct {
	Data unsafe.Pointer
	Len  int
	Cap  int
}

func copyUint32SliceToBytes(dest []byte, source []uint32) {
	src := *(*[]byte)(unsafe.Pointer(&slice{
		Data: unsafe.Pointer(&source[0]),
		Len:  len(source) * 4,
		Cap:  len(source) * 4,
	}))
	copy(dest, src)
}

func copyUint16SliceToBytes(dest []byte, source []uint16) {
	src := *(*[]byte)(unsafe.Pointer(&slice{
		Data: unsafe.Pointer(&source[0]),
		Len:  len(source) * 2,
		Cap:  len(source) * 2,
	}))
	copy(dest, src)
}
