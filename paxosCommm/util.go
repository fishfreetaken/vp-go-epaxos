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

type PaBitCount struct {
	cnt uint32
	bit []uint32 //统计数量，用来去重判断返回的用户
}

func (m *PaBitCount) Add(fromid uint32) bool {

	pos := fromid / 8
	idx := fromid % 8

	for int(pos) >= len(m.bit) {
		m.bit = append(m.bit, 0)
	}
	if (m.bit[pos-1] & (1 << idx)) > 0 {
		return false
	}
	m.bit[pos-1] = m.bit[pos-1] & (1 << idx)
	m.cnt++
	return true
}

func (m *PaBitCount) ExceedHalf(membernum uint32) bool {
	h := membernum >> 1
	if m.cnt < h {
		return false
	}
	return true
}

func isImpossible(membernum, curCount, maxNum, passNum uint32) bool {
	if (membernum - curCount + maxNum) < passNum {
		return true
	}
	return false
}
