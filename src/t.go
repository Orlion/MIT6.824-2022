package main

import (
	"fmt"
	"time"
)

func main() {
	var lastHeartbeatTime time.Time

	if time.Now().Sub(lastHeartbeatTime) < heartbeatTime() {
		fmt.Println("a")
	} else {
		fmt.Println("b")
	}
}

func heartbeatTime() time.Duration {
	return 100 * time.Millisecond
}
