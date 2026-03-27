//go:build go1.17
// +build go1.17

package parser

import "unsafe"

func Copy(source unsafe.Pointer, data []byte, index int, length int) {
	dst := data[index : index+length]
	src := unsafe.Slice((*byte)(source), length)
	copy(dst, src)
}
