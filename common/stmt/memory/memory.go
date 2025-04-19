package memory

var (
	memset func(b []byte, c byte) = memory_memset_go
)

// Set assigns the value c to every element of the slice buf.
func Set(buf []byte, c byte) {
	memset(buf, c)
}

// memory_memset_go reference implementation
func memory_memset_go(buf []byte, c byte) {
	for i := 0; i < len(buf); i++ {
		buf[i] = c
	}
	//if len(buf) < 16 {
	//	for i := 0; i < len(buf); i++ {
	//		buf[i] = c
	//	}
	//}
	//u64 := []byte{c, c, c, c, c, c, c, c}
	//u64Val := *(*uint64)(unsafe.Pointer(&u64[0]))
	//for i := 0; i < len(buf)-8; i += 8 {
	//	*(*uint64)(unsafe.Pointer(&buf[i])) = u64Val
	//}
	//// Handle the remaining bytes
	//for i := len(buf) - len(buf)%8; i < len(buf); i++ {
	//	buf[i] = c
	//}
}
