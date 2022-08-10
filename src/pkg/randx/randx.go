package randx

import (
	"math/rand"
	"time"
)

func Intn(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	return rand.Intn(max-min) + min
}
