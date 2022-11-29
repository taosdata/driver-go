package common

import (
	"testing"
)

func TestGetReqId(t *testing.T) {
	GetReqId()
}

func BenchmarkGetReqId(b *testing.B) {
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		GetReqId()
	}
}
