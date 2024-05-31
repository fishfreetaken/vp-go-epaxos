package paxoscommm

import (
	"math/rand"
	"time"
)

func GetPercent(a, b int) float32 {
	return float32(a*100) / float32(b)
}

func randsleep() {
	t := rand.Intn(100)
	time.Sleep(time.Duration(t) * time.Millisecond)
}
