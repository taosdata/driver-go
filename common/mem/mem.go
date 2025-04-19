package mem

import "unsafe"

//go:noescape
func memmove(to, from unsafe.Pointer, n uintptr)

//go:linkname memmove runtime.memmove

func Copy(source unsafe.Pointer, data []byte, index int, length int) {
	memmove(unsafe.Pointer(&data[index]), source, uintptr(length))
}

func CopyUncheck(source unsafe.Pointer, target unsafe.Pointer, length int) {
	memmove(target, source, uintptr(length))
}
