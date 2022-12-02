package common

import (
	"github.com/google/uuid"
)

func init() {
	uuid.EnableRandPool()
}

// GetReqID get a unique id.
func GetReqID() int64 {
	uid, _ := uuid.NewRandom()
	return int64(uid.ID())
}
