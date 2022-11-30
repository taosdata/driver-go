package common

import (
	"testing"
)

func BenchmarkGetReqID(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetReqID()
	}
}
