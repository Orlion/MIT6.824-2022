package raft

import (
	"log"
	"time"

	"6.824/pkg/randx"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func randElectronTimeout() time.Duration {
	return time.Duration(randx.Intn(150, 300)) * time.Millisecond
}

func heartbeatTime() time.Duration {
	return 100 * time.Millisecond
}