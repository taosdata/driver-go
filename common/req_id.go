package common

import (
	"os"
	"os/signal"
	"time"

	"github.com/google/uuid"
)

func init() {
	uuid.EnableRandPool()
	defaultSerialNo = newSerialNo()
}

// GetReqId get a unique id.
func GetReqId() int64 {
	uid, _ := uuid.NewRandom()
	ts := timestamp() >> 8
	sno := defaultSerialNo.getSerialNo()
	pid := os.Getpid()

	return (int64(uid.ID()) & 0x07FF << 52) | ((int64(pid) & 0x0F) << 48) | ((ts & 0x3FFFFF) << 20) | (sno & 0xFFFFF)
}

func timestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

var defaultSerialNo *serialNo

type serialNo struct {
	ch   chan int64
	done chan os.Signal
}

func newSerialNo() *serialNo {
	s := &serialNo{ch: make(chan int64, 10), done: make(chan os.Signal)}
	signal.Notify(s.done, os.Interrupt)
	go func() {
		defer close(s.ch)
		var i int64
		for {
			select {
			case <-s.done:
				return
			default:
				s.ch <- i
			}
			i++
			if i > 0xFFFFF {
				i = 0
			}
		}
	}()
	return s
}

func (s *serialNo) getSerialNo() int64 {
	return <-s.ch
}
