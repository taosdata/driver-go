package common

import (
	"os"
	"path/filepath"
	"sync"
)

var processName string
var once sync.Once

func GetProcessName() string {
	once.Do(func() {
		processPath := os.Args[0]
		processName = filepath.Base(processPath)
		if processName == "" {
			processName = "go_unknown"
		}
	})
	return processName
}
