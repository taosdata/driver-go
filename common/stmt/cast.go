package stmt

import (
	"unsafe"
)

type sliceHeader struct {
	data unsafe.Pointer
	len  int
	cap  int
}

func Int8CastFromBytes(b []byte) (out []int8) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap
	hdr.len = inHdr.len
	return out
}

func Uint8CastFromBytes(b []byte) (out []uint8) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap
	hdr.len = inHdr.len
	return out
}

func Int16CastFromBytes(b []byte) (out []int16) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 2
	hdr.len = inHdr.len / 2
	return out
}

func Uint16CastFromBytes(b []byte) (out []uint16) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 2
	hdr.len = inHdr.len / 2
	return out
}

func Int32CastFromBytes(b []byte) (out []int32) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 4
	hdr.len = inHdr.len / 4
	return out
}

func Uint32CastFromBytes(b []byte) (out []uint32) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 4
	hdr.len = inHdr.len / 4
	return out
}

func Int64CastFromBytes(b []byte) (out []int64) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 8
	hdr.len = inHdr.len / 8
	return out
}

func Uint64CastFromBytes(b []byte) (out []uint64) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 8
	hdr.len = inHdr.len / 8
	return out
}

func BoolCastFromBytes(b []byte) (out []bool) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 1
	hdr.len = inHdr.len / 1
	return out
}

func Float32CastFromBytes(b []byte) (out []float32) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 4
	hdr.len = inHdr.len / 4
	return out
}

func Float64CastFromBytes(b []byte) (out []float64) {
	if cap(b) == 0 {
		return nil
	}
	inHdr := (*sliceHeader)(unsafe.Pointer(&b))
	hdr := (*sliceHeader)(unsafe.Pointer(&out))
	hdr.data = inHdr.data
	hdr.cap = inHdr.cap / 8
	hdr.len = inHdr.len / 8
	return out
}
