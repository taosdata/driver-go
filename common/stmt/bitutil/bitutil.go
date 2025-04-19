package bitutil

import "math/bits"

func NextPowerOf2(x int) int { return 1 << uint(bits.Len(uint(x))) }
