package paxoscommm

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

type PaGroup struct {
	list []*PaNode

	recvseq  chan VoteInfo
	mpResult map[int64]int //seq -> nodeid
	begin    time.Time

	decidenum   int
	nodecidenum int
	invalid     int
}

func (m *PaGroup) Sendto(idx int, st PaCommnMsg) {
	m.list[idx].Recv(st)
}

func (m *PaGroup) Index(idx int) *PaNode {
	return m.list[idx]
}

func (m *PaGroup) Broadcast(st *PaCommnMsg) {
	m.Broadcastexcept(*st, -1)
}

func (m *PaGroup) Broadcastexcept(st PaCommnMsg, index int) {
	for i := range m.list {
		if i == index {
			continue
		}
		m.Sendto(i, st)
	}
}

func (m *PaGroup) GetCurMaxSeq() int64 {
	var res int64
	for _, v := range m.list {
		if atomic.LoadInt64(&v.curseq) > res {
			res = atomic.LoadInt64(&v.curseq)
		}
	}
	return res
}

func (m *PaGroup) Report(seq int64) {
	var rc = make(map[int][]int)
	for _, v := range m.list {
		if !v.GetSeqMsg(seq).IsAccept() {
			//没有accept就不需要统计了
			continue
		}
		rc[v.GetSeqMsg(seq).Vt.AcceptVote] = append(rc[v.GetSeqMsg(seq).Vt.AcceptVote], v.GetId())
	}

	masterIdx := -1
	for key, v := range rc {
		if key == 0 {
			panic(fmt.Sprintf("invalid key seq:%d", seq))
		}
		if len(v) >= (len(m.list)>>1 + 1) {
			if masterIdx != -1 {
				panic("invalid master index")
			}
			masterIdx = key
		}
	}

	var printRc = func() {
		for key, v := range rc {
			fmt.Printf("vote:%d list:%+v \n", key, v)
			fmt.Printf("key:%d msg:%+v\n", key, m.list[key].GetSeqMsg(seq))
		}
	}

	tmplocalgp := -1
	if v, ok := m.mpResult[seq]; ok {
		tmplocalgp = v
	}
	if tmplocalgp == masterIdx && masterIdx != -1 {
		m.decidenum++
		return
	} else if tmplocalgp != masterIdx {
		m.invalid++
		fmt.Printf("node not equal seq:%d localresult:%d masteridx:%d msg:%+v\n", seq, tmplocalgp, masterIdx, m.list[masterIdx].GetSeqMsg(seq))
	} else {
		fmt.Printf("seq:%d not desicde \n", seq)
		m.nodecidenum++
	}
	printRc()
	fmt.Printf("\n")
}

func (m *PaGroup) GetLastCalc(totalseq int) {
	var ttcnt int
	var proposesum int
	for _, v := range m.list {
		fmt.Printf("%s\n", v.CalcLastSuccRate(&ttcnt, &proposesum))
	}
	fmt.Printf("GetLastCalc total:%d:%d:%d sumproposenum:%d:%f decide %d:%f nodecide:%d:%f invalid:%d 时间消耗：%+v\n", totalseq, m.decidenum+m.nodecidenum, ttcnt, proposesum, GetPercent(totalseq, proposesum), m.decidenum, GetPercent(m.decidenum, totalseq), m.nodecidenum, GetPercent(m.nodecidenum, totalseq), m.invalid, time.Since(m.begin))
}

func (m *PaGroup) InformVoteResult(t VoteInfo) {
	//看所有的seq都已经计算过了
	m.recvseq <- t
}

func (m *PaGroup) Wait(seqNum int64) {
	t := time.NewTicker(time.Second * 15)
	for {
		select {
		case v := <-m.recvseq:
			if m.mpResult == nil {
				m.mpResult = make(map[int64]int)
			}
			if hasMasterid, ok := m.mpResult[v.Seq]; ok {
				if hasMasterid != v.AcceptVote {
					panic(fmt.Sprintf("seq:%d hasmaster:%d masterid:%+v :\n %+v \n%+v", v.Seq, hasMasterid, v, m.list[hasMasterid].GetSeqMsg(v.Seq), m.list[v.AcceptVote].GetSeqMsg(v.Seq)))
				}
				break
			}
			//fmt.Printf("recv tvs: seq: %d acceptvote:%d\n", v.Seq, v.AcceptVote)
			m.mpResult[v.Seq] = v.AcceptVote
			if len(m.mpResult) == int(seqNum) {
				//检查是否完成
				fmt.Printf("seq over:%d result:%d\n", seqNum, len(m.mpResult))
				return
			}
		case <-t.C:
			//时间过期了
			t.Stop()
			fmt.Printf("wait expire cntvalue:%d seqnum:%d\n", len(m.mpResult), seqNum)
			return
		}
	}
}

func (m *PaGroup) Init(membernum int) {
	rand.New(rand.NewSource(time.Now().Unix()))
	m.begin = time.Now()
	for i := 0; i < membernum; i++ {
		m.list = append(m.list, &PaNode{
			id:       i,
			priority: i,
			//recv: make(chan PaCommnMsg, 1000),
			//dict: make(map[int64]*PaCommnMsg),
			mpLocPropose: make(map[int64]*AckState),
			seqmap:       make(map[int64]int64),
			g:            m,
		})
		m.list[i].SetVecLkNums()
	}
	m.recvseq = make(chan VoteInfo, 1000)
}

func (m *PaGroup) RandNodeIndex() int {
	return int(rand.Int31()) % len(m.list)
}

func (m *PaGroup) GetNumber() int {
	return len(m.list)
}
