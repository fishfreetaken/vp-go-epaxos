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
func GetPosCount(k uint32) int {
	cnt := 1
	for k > 0 {
		k = k / 10
		cnt *= 10
	}
	return cnt
}

func MaxValue(a, b uint32) uint32 {
	if a > b {
		return a
	}
	return b
}

func BitCount(t uint32) uint32 {
	var cc uint32
	for t > 0 {
		if (t & 1) > 0 {
			cc++
		}
	}
	return cc
}
